/*
 * Copyright 2014 Frugal Mechanic (http://frugalmechanic.com)
 * Copyright 2021 Tim Underwood (https://github.com/tpunder)
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

import java.io.Closeable
import java.util.concurrent.{BlockingQueue, TimeUnit}
import scala.collection.mutable.Builder
import scala.concurrent.duration.FiniteDuration

object BufferedLazySeq {
  def apply[A](reader: LazySeq[A], size: Int): BufferedLazySeq[A] = {
    apply(reader, new LazySeqBuilder[A](size))
  }

  def apply[A](reader: LazySeq[A], queue: BlockingQueue[A]): BufferedLazySeq[A] = {
    apply(reader, new LazySeqBuilder[A](queue))
  }

  def apply[A](reader: LazySeq[A], builder: LazySeqBuilder[A]): BufferedLazySeq[A] = {
    builder.withProducerThread{ growable => reader.foreach{ growable += _ } }
    builder.lazySeq
  }

}

trait BufferedLazySeq[+A] extends LazySeq[A] with Closeable {
  def iterator: BufferedLazySeqIterator[A]

  final def grouped[B >: A](size: Int, duration: FiniteDuration): BufferedGroupedLazySeq[B] = {
    new BufferedGroupedLazySeq(iterator, size, duration.length, duration.unit)
  }

  final def grouped[B >: A](size: Int, timeout: Long, unit: TimeUnit): BufferedGroupedLazySeq[B] = {
    new BufferedGroupedLazySeq(iterator, size, timeout, unit)
  }

  final def grouped[B >: A](size: Int, additionalIncrement: B => Int, duration: FiniteDuration): BufferedGroupedLazySeq[B] = {
    new BufferedGroupedLazySeq(iterator, size, additionalIncrement, duration.length, duration.unit)
  }

  final def grouped[B >: A](size: Int, additionalIncrement: B => Int, timeout: Long, unit: TimeUnit): BufferedGroupedLazySeq[B] = {
    new BufferedGroupedLazySeq(iterator, size, additionalIncrement, timeout, unit)
  }
}

/**
 * Similar to GroupedLazySeq but allows you to specify a max time to wait before processing the current batch
 * @param iterator The underlying BufferedLazySeqIterator to use
 * @param size
 * @param additionalIncrement
 * @param timeout
 * @param unit
 * @tparam A
 */
final class BufferedGroupedLazySeq[+A](self: BufferedLazySeqIterator[A], size: Int, additionalIncrement: A => Int, timeout: Long, unit: TimeUnit) extends LazySeq[IndexedSeq[A]] { outer =>
  def this(iterator: BufferedLazySeqIterator[A], size: Int, timeout: Long, unit: TimeUnit) = this(iterator, size, null, timeout, unit)

  require(size > 0, "size must be > 0 for BufferedGroupedLazySeq")

  private[this] var hd: IndexedSeq[A] = null

  object iterator extends LazySeqIterator[IndexedSeq[A]] {
    override def close(): Unit = self.close()
    override def hasNext: Boolean = outer.hasNext
    override def head: IndexedSeq[A] = outer.head
    override def next(): IndexedSeq[A] = outer.next()
  }

  override def head: IndexedSeq[A] = {
    if (!hasNext) throw new NoSuchElementException("No more elements in iterator") else hd
  }

  def next(): IndexedSeq[A] = {
    if (!hasNext) throw new NoSuchElementException("No more elements in iterator")
    val res: IndexedSeq[A] = hd
    hd = null
    res
  }

  def hasNext: Boolean = {
    if (null == hd) {

      // We need at least 1 element before we return or if self.hasNext returns false then we are at the end of the queue
      if (self.hasNext) {
        var count: Int = 0
        val buf: Builder[A, Vector[A]] = Vector.newBuilder[A]

        def add(elem: A): Unit = {
          buf += elem
          val increment: Int = 1 + (if (null == additionalIncrement) 0 else additionalIncrement(elem))
          assert(increment > 0, "AdditionalIncrement must be positive: "+increment)
          count += increment
        }

        add(self.next())

        val start: Long = System.nanoTime()
        var remaining: Long = remainingNanos(start)

        while (count < size && remaining > 0L) {
          self.hasNext(remaining, TimeUnit.NANOSECONDS) match {
            case Some(true)  =>
              add(self.next()) // Check for next
              remaining = remainingNanos(start)

            case Some(false) => remaining = 0L // End of queue - we should exit loop and process current batch
            case None        => remaining = 0L // Timed out    - we should exit loop and process current batch
          }
        }

        hd = buf.result()
      }
    }

    null != hd
  }

  override def foreach[U](f: IndexedSeq[A] => U): Unit = {
    while (hasNext) {
      f(next())
    }
  }

  private def remainingNanos(startNanos: Long): Long = {
    val elapsed: Long = System.nanoTime() - startNanos
    unit.toNanos(timeout) - elapsed
  }
}
