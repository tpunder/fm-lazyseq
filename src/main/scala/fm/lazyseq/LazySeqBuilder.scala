/*
 * Copyright 2014 Frugal Mechanic (http://frugalmechanic.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fm.lazyseq

import fm.common.Logging
import fm.common.Implicits._
import java.io.Closeable
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, CountDownLatch, SynchronousQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.generic.Growable
import scala.collection.mutable.Builder
import scala.concurrent.duration.FiniteDuration

object LazySeqBuilder {
  final class AbortedException() extends Exception("LazySeqBuilder has been aborted!")

  private val MaxPollTimeMillis: Int = 100
  
  private[this] val _uniqueIdAtomicInteger: AtomicInteger = new AtomicInteger(0)
  private def nextUniqueId(): Int = _uniqueIdAtomicInteger.incrementAndGet()
}

/**
 * A LazySeq producer/consumer pair that uses a BlockingQueue
 * 
 * I think this is Thread-Safe
 *
 * 2021-02-19 - This was updated to allow passing in an arbitrary BlockingQueue implementation (e.g. an
 *              LMDBBlockingDeque for using an off-heap disk-backed queue). I removed the END_OF_QUEUE marker so that
 *              the BlockingQueue could be typed as a BlockingQueue[A] instead of an BlockingQueue[AnyRef] to make
 *              serialization in LMDBBlockingDeque work better.
 *
 *              It looks like there is a lot of cleanup potential in this class. After some original threading problems
 *              around handling exceptions in the Producer and Consumer threads it looks like I went a little overboard
 *              on error handling. There is probably room for a lot of simplification.
 */
final class LazySeqBuilder[A](queue: BlockingQueue[A], shutdownJVMOnUncaughtException: Boolean = false) extends Builder[A, LazySeq[A]] with Closeable with Logging { builder =>
  def this(queueSize: Int, shutdownJVMOnUncaughtException: Boolean) = this(if (queueSize > 0) new ArrayBlockingQueue[A](queueSize) else new SynchronousQueue[A](), shutdownJVMOnUncaughtException)
  def this(queueSize: Int) = this(queueSize, false)
  def this() = this(16, false)

  import LazySeqBuilder.{AbortedException, MaxPollTimeMillis}
  
  protected val uniqueId: Int = LazySeqBuilder.nextUniqueId()
  
  private class Aborted extends Throwable

  def queueSize: Int = queue.size()
  
  def +=(v: A): this.type = {
    abortCheck()
    
    if (closed) closedWarning
    else if (Thread.interrupted()) throw new InterruptedException
    else {
      while (!closed && !aborting && !queue.offer(v, MaxPollTimeMillis, TimeUnit.MILLISECONDS)) {
        // Loop until we successfully insert into the queue (and are not closed or aborting)
      }
    }
    
    abortCheck()
    
    this
  }
  
  def result(): LazySeq[A] = lazySeq
  
  def clear(): Unit = ???
  
  private lazy val closedWarning: Unit = {
    logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.error("Trying to add to closed LazySeqBuilder.  Unless the app is abnormally terminating, this is an error!")
    logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  }
  
  @volatile private[this] var closed = false
  @volatile private[this] var aborting = false
  @volatile private[this] var producerThread: ProducerThread = null
  @volatile private[this] var consumerThread: ConsumerThread = null
  
  private[this] val closingOrAbortingProducer = new AtomicBoolean(false)
  private[this] val closingOrAbortingConsumer = new AtomicBoolean(false)
  
  private[this] val producerThreadCreated = new AtomicBoolean(false)
  private[this] val consumerThreadCreated = new AtomicBoolean(false)
  
  protected final def isProducerOrConsumerThread(): Boolean = {
    val currentThread: Thread = Thread.currentThread()
    currentThread === producerThread || currentThread === consumerThread
  }
  
  private[this] final def abortCheck(): Unit = {
    if (aborting) {
      if (isProducerOrConsumerThread()) throw new Aborted()
      else throw new AbortedException()
    }
  }
  
  /**
   * Note: It's not clear what the correct behavior of this method is.
   *       It will currently block until the producer/consumer threads
   *       have cleanly finished.
   */
  def close(): Unit = {
    logger.debug(s"${uniqueId} close() ...")
    closeProducer()
    closeConsumer()
    logger.debug(s"${uniqueId} close() ... DONE!")
  }
  
  /**
   * This will abort both the producer and consumer possibly removing
   * items from the queue to make room for our END_OF_QUEUE marker.
   */
  def abort(): Unit = {
    logger.debug(s"${uniqueId} abort() ...")
    abortProducer()
    abortConsumer()
    logger.debug(s"${uniqueId} abort() ... DONE!")
  }
  
  def closeProducer(): Unit = {
    // Only call closeProducer()/abortProducer() once
    if (!closingOrAbortingProducer.compareAndSet(false, true)) return
    
    logger.debug(s"${uniqueId} closeProducer()")
    closed = true
    
    val pt: ProducerThread = producerThread
    
    if (null != pt && Thread.currentThread() != pt) {
      logger.debug(s"${uniqueId} closeProducer() - Waiting for ProducerThread to finish ...")
      pt.latch.await()
      logger.debug(s"${uniqueId} closeProducer() - Waiting for ProducerThread to finish ... DONE!")
    }
    
    // Clear the reference to the producer thread
    producerThread = null
    logger.debug(s"${uniqueId} closeProducer() ... DONE!")
  }
  
  def abortProducer(): Unit = {
    // Only call closeProducer()/abortProducer() once
    if (!closingOrAbortingProducer.compareAndSet(false, true)) return
    
    logger.debug(s"${uniqueId} abortProducer() ...")

    aborting = true
    closed = true
    
    val pt: ProducerThread = producerThread
    if (null != pt && Thread.currentThread() != pt) {
      // Interrupt the producer thread
      pt.interrupt()
      
      logger.debug(s"${uniqueId} abortProducer() - Waiting for ProducerThread to finish ...")
      
      while (!pt.latch.await(5, TimeUnit.SECONDS)) {
        logger.warn(s"Still Waiting for ProducerThread to finish.  Expected toe pt.interrupt() call to cause it to finish.")
        pt.interrupt()
      }
      
      logger.debug(s"${uniqueId} abortProducer() - Waiting for ProducerThread to finish ... DONE!")
    }
    
    // Clear the reference to the producer thread
    producerThread = null
    
    logger.debug(s"${uniqueId} abortProducer() ... DONE!")
  }
  
  def closeConsumer(): Unit = {
    // Only call closeConsumer()/abortConsumer() once
    if (!closingOrAbortingConsumer.compareAndSet(false, true)) return
    
    logger.debug(s"${uniqueId} closeConsumer() ...")
    
    val ct: ConsumerThread = consumerThread
    
    if (null != ct && Thread.currentThread() != ct) {
      logger.debug(s"${uniqueId} closeConsumer() - Waiting for the ConsumerThread to finish ...")
      ct.latch.await()
      logger.debug(s"${uniqueId} closeConsumer() - Waiting for the ConsumerThread to finish ... DONE!")
    }
    
    // Clear the reference to the consumer thread
    consumerThread = null
    logger.debug(s"${uniqueId} closeConsumer() ... DONE!")
  }
  
  def abortConsumer(): Unit = {
    // Only call closeConsumer()/abortConsumer() once
    if (!closingOrAbortingConsumer.compareAndSet(false, true)) return
    
    logger.debug(s"${uniqueId} abortConsumer() ...")
    
    aborting = true

    val ct: ConsumerThread = consumerThread
    if (null != ct && Thread.currentThread() != ct) {
      // Interrupt the consumer thread
      ct.interrupt()
      
      logger.debug(s"${uniqueId} abortConsumer() - Waiting for the ConsumerThread to finish ...")
      
      // Wait for the consumer thread to finish
      while (!ct.latch.await(5, TimeUnit.SECONDS)) {
        logger.warn(s"${uniqueId} Still Waiting for ConsumerThread to finish.  Expected toe ct.interrupt() call to cause it to finish.")
        ct.interrupt()
      }
      
      logger.debug(s"${uniqueId} abortConsumer() - Waiting for the ConsumerThread to finish ... DONE!")
    }
    
    // Clear the reference to the consumer thread
    consumerThread = null
    
    // If there is anything remaining in the queue lets drain it to unblock any producers
    if (null != queue.poll()) {
      while (null != queue.poll(MaxPollTimeMillis, TimeUnit.MILLISECONDS)) { }
    }

    logger.debug(s"${uniqueId} abortConsumer() ... DONE!")
  }
  
  object lazySeq extends BufferedLazySeq[A] with Closeable { reader =>
    private[this] var hd: AnyRef = null
    private[this] var hdDefined: Boolean = false
    
    override def head: A = {
      if (!hasNext) throw new NoSuchElementException("No more elements in iterator")
      hd.asInstanceOf[A]
    }
    
    override def headOption: Option[A] = if (hasNext) Some(head) else None
    
    def next: A = {      
      if (!hasNext) throw new NoSuchElementException("No more elements in iterator")
      val res: A = hd.asInstanceOf[A]
      hd = null
      hdDefined = false
      res
    }
    
    def hasNext: Boolean = {
      abortCheck()
      
      if (!hdDefined) {

        // Try first without waiting to see if there is anything in the queue. This handles the case of where we are
        // closed (i.e. no more writes are happening to the queue) and we just want to quickly check if there is
        // anything left in the queue.
        hd = queue.poll().asInstanceOf[AnyRef]

        // If we got nothing from the queue then (as long as we are not closed or aborting) we loop checking for new
        // data in the queue until we either get something or the closed and/or aborting flags are set. We wait a max
        // of 100ms for new data before re-checking the closed and aborting flags.
        while (null == hd && !closed && !aborting) {
          hd = queue.poll(MaxPollTimeMillis, TimeUnit.MILLISECONDS).asInstanceOf[AnyRef]
        }

        // If we got back null (due to a timeout) and the closed or aborting flags are now set to true then let's
        // check one more time to make sure we didn't missing something in the queue (e.g. between the time the timeout
        // happened and any closed/aborting flag got set).
        if (null == hd && (closed || aborting)) {
          hd = queue.poll().asInstanceOf[AnyRef]
        }

        // There are 2 possible states here:
        // null == hd - We have reached the end of the Queue and do not need to check it again.
        // null != hd - We successfully read from the queue. hdDefined will be set to false when next is called.
        hdDefined = true
        abortCheck()
      }

      null != hd
    }

    def hasNext(duration: FiniteDuration): Option[Boolean] = hasNext(duration.length, duration.unit)

    /**
     * A version of hasNext with a timeout
     * @param timeout
     * @param unit
     * @return None if we hit our timeout, otherwise a Some instance with the actual result
     */
    def hasNext(timeout: Long, unit: TimeUnit): Option[Boolean] = {
      abortCheck()

      // If hd is already defined then there is nothing for us to do
      if (hdDefined) return Some.cached(null != hd)

      hd = queue.poll(timeout, unit).asInstanceOf[AnyRef]

      // If hd is null here then we hit the timeout. If we are also closed or aborting let's do a final poll() to make
      // sure we didn't missing anything.
      if (null == hd && (closed || aborting)) {
        hd = queue.poll(timeout, unit).asInstanceOf[AnyRef]
        // If we are still null then we have hit the end of the queue
        if (null == hd) {
          hdDefined = true
          return Some.cached(false)
        }
      }

      // If we are null at this point then we timed out, otherwise we have a value set
      if (null == hd) {
        None
      } else {
        hdDefined = true
        Some.cached(true)
      }
    }
    
    def close(): Unit = {
      builder.close()
    }
    
    object iterator extends BufferedLazySeqIterator[A] {
      override def hasNext: Boolean = reader.hasNext
      override def hasNext(duration: FiniteDuration): Option[Boolean] = reader.hasNext(duration)
      override def hasNext(timeout: Long, unit: TimeUnit): Option[Boolean] = reader.hasNext(timeout, unit)
      override def head: A = reader.head
      override def next: A = reader.next
      override def close(): Unit = reader.close()
    }
    
    final def foreach[U](f: A => U): Unit = try {
      while (hasNext) f(next)
    } catch {
      case ex: Throwable => 
        logger.warn("Caught Exception running LazySeqBuilder.foreach().  Aborting...", ex)
        builder.abort()
        throw ex
    }
  }
  
  /**
   * Run the function in a separate producer thread.  Should only be called once since
   * it will close the Builder when the thread finishes.
   */
  def withProducerThread(f: Growable[A] => Unit): this.type = {
    require(producerThreadCreated.compareAndSet(false, true), "withProducerThread already called!")
    
    require(producerThread == null, "producerThread should be null")
    
    producerThread = new ProducerThread(f)
    producerThread.start()
    
    this
  }
  
  def withConsumerThread(f: LazySeq[A] => Unit): this.type = {
    require(consumerThreadCreated.compareAndSet(false, true), "withConsumerThread already called!")
    
    require(consumerThread == null, "consumerThread should be null")
    
    consumerThread = new ConsumerThread(f)
    consumerThread.start()
    
    this
  }
  
  /**
   * Wait for both the producer and consumer threads (if any) to finish
   */
  def await() {
    awaitProducer()
    awaitConsumer()
  }
  
  /**
   * Wait for the producer thread (if any) to finish
   */
  def awaitProducer() {
    logger.debug(s"${uniqueId} awaitProducer() ...")
    val pt: ProducerThread = producerThread
    if (null != pt) {
      require(pt != Thread.currentThread(), "awaitProducer() - Can't wait on our own thread!")
      pt.latch.await()
    }
    logger.debug(s"${uniqueId} awaitProducer() ... DONE!")
  }
  
  /**
   * Wait for the consumer thread (if any) to finish
   */
  def awaitConsumer() {
    logger.debug(s"${uniqueId} awaitConsumer() ...")
    val ct: ConsumerThread = consumerThread
    if (null != ct) {
      require(ct != Thread.currentThread(), "awaitConsumer() - Can't wait on our own thread!")
      ct.latch.await()
    }
    logger.debug(s"${uniqueId} awaitConsumer() ... DONE!")
  }
  
  private class ProducerThread(f: Growable[A] => Unit) extends Thread(s"RR-Builder-Producer-${uniqueId}") {
    setDaemon(true)
    val latch: CountDownLatch = new CountDownLatch(1)
    
    override def run(): Unit = try {
      if (closed || aborting) return
      
      logger.debug(s"${uniqueId} ProducerThread.run() ...")
      f(builder)
      logger.debug(s"${uniqueId} ProducerThread.run() finished running f(builder), now waiting for close() to complete...")
      builder.close()
      logger.debug(s"${uniqueId} ProducerThread.run() ... DONE!")
    } catch {
      case ex: Aborted =>                    // Expected if we are aborting
      case ex: InterruptedException =>       // Expected if we are aborting
      case ex: ClosedByInterruptException => // Expected if we are aborting
      case ex: Throwable =>
        logger.warn("ProducerThread Caught Throwable - Aborting Consumer ...", ex)
        builder.abort()
        throw ex
    } finally {
      latch.countDown()
    }
    
    // This shouldn't be called, but it's here just in case
    setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler{
      def uncaughtException(t: Thread, e: Throwable) {
        logger.error("Uncaught Exception", e)
        if (shutdownJVMOnUncaughtException) {
          logger.error("Shutting down JVM")
          Runtime.getRuntime.exit(-1)
        }
      }
    })
  }
  
  private class ConsumerThread(f: LazySeq[A] => Unit) extends Thread(s"RR-Builder-Consumer-${uniqueId}") {
    setDaemon(true)
    val latch: CountDownLatch = new CountDownLatch(1)
    
    override def run(): Unit = try {
      if (aborting) return
      
      logger.debug(s"${uniqueId} ConsumerThread.run() ...")
      f(lazySeq)
      logger.debug(s"${uniqueId} ConsumerThread.run() ... DONE!")
    } catch {
      case ex: Aborted =>                    // Expected if we are aborting
      case ex: InterruptedException =>       // Expected if we are aborting
      case ex: ClosedByInterruptException => // Expected if we are aborting
      case ex: Throwable =>
        logger.warn("ConsumerThread Caught Throwable - Aborting Producer ...", ex)
        builder.abort()
        throw ex
    } finally {
      latch.countDown()
    }
    
    // This shouldn't be called, but it's here just in case
    setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler{
      def uncaughtException(t: Thread, e: Throwable) {
        logger.error("Uncaught Exception", e)
        if (shutdownJVMOnUncaughtException) {
          logger.error("Shutting down JVM")
          Runtime.getRuntime.exit(-1)
        }
      }
    })
  }

}