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

import fm.common.Implicits._
import fm.common.{ByteBufferInputStream, ByteBufferUtil, Logging, ProgressStats, Resource, Serializer, Snappy, TaskRunner, UncloseableOutputStream}
import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.util.{Arrays, Comparator, PriorityQueue}
import scala.collection.mutable.Builder
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

private object SortedLazySeqBuilder {
  def DefaultBufferSizeLimitMB: Int = 100
  def DefaultBufferRecordLimit: Int = 250000
  def DefaultSortAndSaveThreads: Int = 1
  def DefaultSortAndSaveQueueSize: Int = 1
}

/**
 * Keeps an in-memory buffer gets sorted once a size threshold is passed and written to a temp file.  This is repeated until
 * result() is called.  Data is then read back from the temp files in sorted order.
 * 
 * This should be thread-safe
 */
final private class SortedLazySeqBuilder[V, K](key: V => K, unique: Boolean = false, bufferSizeLimitMB: Int = SortedLazySeqBuilder.DefaultBufferSizeLimitMB, bufferRecordLimit: Int = SortedLazySeqBuilder.DefaultBufferRecordLimit, sortAndSaveThreads: Int = SortedLazySeqBuilder.DefaultSortAndSaveThreads, sortAndSaveQueueSize: Int = SortedLazySeqBuilder.DefaultSortAndSaveQueueSize)(implicit serializer: Serializer[V], ord: Ordering[K]) extends Builder[V, LazySeq[V]] with Logging {
  import serializer._
  
  // Debug flag to disable deletion of tmp files
  private[this] val deleteTmpFiles: Boolean = true
  
  // How large of a buffer to use for storing records before sorting and writing to a tmp file (does not take into account the size of the key)
  private[this] val bufferSizeLimitBytes: Int = bufferSizeLimitMB * 1024 * 1024
  
  private case class KeyBytesPair(key: K, bytes: Array[Byte])
  
  private[this] val sortAndSaveTaskRunner = TaskRunner(name="SortedLazySeq-SortAndSave", threads=sortAndSaveThreads, queueSize=sortAndSaveQueueSize)
  private[this] val stats = ProgressStats.forFasterProcesses()
    
  private[this] var buffer: Array[KeyBytesPair] = new Array(bufferRecordLimit)
  private[this] var bufferSizeBytes: Int = 0
  private[this] var count: Int = 0
  private[this] val sortAndSaveFutures = Vector.newBuilder[Future[Vector[MappedByteBuffer]]]
  
  @volatile private[this] var done: Boolean = false
  
  private[this] val keyBytesOrdering: Ordering[KeyBytesPair] = Ordering.by[KeyBytesPair, K]{ _.key }
  
  override def ++=(xs: TraversableOnce[V]): this.type = synchronized {
    xs.foreach{ += }
    this
  }
  
  def +=(v: V): this.type = synchronized {
    require(!done, "Already produced result!  Cannot add additional elements!")
    
    val keyBytes: KeyBytesPair = KeyBytesPair(key(v), serialize(v))
    
    buffer(count) = keyBytes
    bufferSizeBytes += keyBytes.bytes.length
    count += 1
    if (count >= bufferRecordLimit || bufferSizeBytes > bufferSizeLimitBytes) flush()
    
    stats.increment()
    this
  }
  
  def result: LazySeq[V] = synchronized {
    require(!done, "Already produced result!")
    done = true
    
    if(count > 0) flush()
    
    sortAndSaveTaskRunner.shutdown()
    
    stats.finalStats
    
    val files: Vector[Vector[MappedByteBuffer]] = sortAndSaveFutures.result.map{ Await.result(_, Duration.Inf) }
    
    if(files.isEmpty) {
      LazySeq.empty
    } else {
      val tmp = new ReadSortedRecords(files)
      if(unique) tmp.uniqueUsing[K](key) else tmp
    }
  }
  
  def clear: Unit = throw new UnsupportedOperationException()
    
  private def flush() {
    // The result needs to be calculated outside of the sortAndSave task
    val result: Array[KeyBytesPair] = Arrays.copyOf(buffer, count)
    
    sortAndSaveFutures += sortAndSaveTaskRunner.submit{ sortAndSave(result) }
    
    // Clear the buffer and counts
    Arrays.fill(buffer.asInstanceOf[Array[Object]], 0, count, null)
    bufferSizeBytes = 0
    count = 0
  }
  
  private def sortAndSave(buffer: Array[KeyBytesPair]): Vector[MappedByteBuffer] = {
    Arrays.sort(buffer, keyBytesOrdering)
  
    val tmpFile: File = File.createTempFile("FmUtilSortedLazySeq", ".compressed")
    
    // Open the file using RandomAccessFile
    val raf = new RandomAccessFile(tmpFile, "rw")

    //
    // NOTE: DO NOT USE File.deleteOnExit() since it uses an append-only LinkedHashSet
    //
    // Instead we use a RandomAccessFile to retain a reference to the FileDescriptor
    // (which we can use to open a FileInputStream).  Then when RandomAccessFile is
    // garbage collected, closed, or the JVM shuts down the file will be deleted.
    //
    
    // Unlink the file from the file system
    if (deleteTmpFiles) tmpFile.delete()
    
    val os: DataOutputStream = new DataOutputStream(Snappy.newSnappyOrGzipOutputStream(UncloseableOutputStream(new FileOutputStream(raf.getFD))))

    try {
      var i: Int = 0
      while (i < buffer.length) {
        val keyValue: KeyBytesPair = buffer(i)
        os.writeInt(keyValue.bytes.length) // 4 bytes - Size of the data
        os.write(keyValue.bytes)           // Actual data
        buffer(i) = null                   // Clear the reference to the KeyBytesPair so it can potentially be GC'd sooner
        i += 1
      }
    } finally {
      os.flush()
      os.close()
    }

    // We use a MappedByteBuffer because if we use "new FileInputStream(raf.getFD)" then reading from
    // the FileInputstream updates the position in the RandomAccessFile which is not what we want.
    // The MappedByteBuffer (which we duplicate() before reading) should make us thread-safe for reading
    // by multiple threads and not require us to reset the RandomAccessFile position before re-reading.
    val bufs: Vector[MappedByteBuffer] = ByteBufferUtil.map(raf, FileChannel.MapMode.READ_ONLY)
    
    raf.close() // Should be safe to close out the RandomAccessFile
    bufs
  }
  
  /**
   * A reusable reader that performs a merge sort on the sorted files
   * 
   * Note: We use RandomAccessFile to hold onto the FileDescriptor of the temp file and open
   *       FileInputStreams based on the FileDescriptor.  This allows us to delete the unlink
   *       the temp file from the file system while retaining a reference to it here so we
   *       can still read it.  Then when the FileDescriptor gets garbage collected OR the JVM
   *       shuts down the file will be automatically deleted.
   */
  final class ReadSortedRecords(files: Vector[Vector[MappedByteBuffer]]) extends LazySeq[V] with Closeable {
    
    def foreach[U](f: V => U): Unit = Resource.using(makeBufferedRecordReaders()) { readers =>
      val pq: PriorityQueue[BufferedRecordReader] = makePriorityQueue()

      readers.foreach{ r => if(!r.isEmpty) pq.add(r) }
      
      while(pq.size > 0) {
        val reader: BufferedRecordReader = pq.poll()
        val value: V = reader.pop()
         
        if(reader.isEmpty) reader.close() else pq.add(reader)
        
        f(value)
      }
    }
    
    private def makeBufferedRecordReaders(): Seq[BufferedRecordReader] = files.map { f => new BufferedRecordReader(f) }
    
    private def makePriorityQueue() = new PriorityQueue[BufferedRecordReader](files.length,
      new Comparator[BufferedRecordReader] {
         def compare(a: BufferedRecordReader, b: BufferedRecordReader):Int = {
           ord.compare(a.headKey, b.headKey)
         }
      }
    )
    
    def close(): Unit = {
      // If deleteTmpFiles is true then this should also trigger a delete since the temp files are already unlinked from the file system
      //logger.info("Closing temp sort files")
      //files.foreach{ _.close() }
    }
    
    // Note: this is needed since RandomAccessFile does not have a finalize method
    override protected def finalize(): Unit = close()
  }
  
  /**
   * Reads records back in from the sorted tmp file.  This class is statefull and should be wrapped
   * by something that will handle automatically closing it
   */
  private final class BufferedRecordReader(bufs: Vector[MappedByteBuffer]) extends Closeable {    
    private[this] val is: DataInputStream = new DataInputStream(Snappy.newSnappyOrGzipInputStream(ByteBufferInputStream(bufs)))
    private[this] var _isEmpty: Boolean = false
    
    private[this] var _cache: V = _
    private[this] var _keyCache: K = _
    
    reload()

    def isEmpty: Boolean = _isEmpty
    
    /**
     * Similar to InputStream.read but ensures that the byte array is 
     * completely filled up or an EOF exception is thrown.  InputStream.read
     * only returns UP TO bytes.length so it has to be called multiple times
     */
    private def readBytesFromInputStream(size: Int): Array[Byte] = {
      assert(size > 0, "Length is 0?")
      
      val bytes: Array[Byte] = new Array[Byte](size)
      
      var totalBytesRead: Int = 0
      while(totalBytesRead < size) {
        val n = is.read(bytes, totalBytesRead, size - totalBytesRead)
        if(-1 === n) {
          if(totalBytesRead > 0) logger.error("Unexpected EOFException.  Expected to read "+size+" bytes but only got "+totalBytesRead+" bytes before EOF")
          throw new EOFException()
        }
        totalBytesRead += n
      }
      
      bytes
    }

    def reload(): Unit = {
      try {
        // File Format:  {Length - 4 Bytes}{Data}...          
        val length: Int = is.readInt()
        val bytes: Array[Byte] = readBytesFromInputStream(length)
        
        _cache = deserialize(bytes)

        if(null == _cache) {
          _isEmpty = true
          _keyCache = null.asInstanceOf[K]
        } else {
          _keyCache = key(_cache)
        }

      } catch {
        case eof: EOFException =>
          _isEmpty = true
          _cache = null.asInstanceOf[V]
          _keyCache = null.asInstanceOf[K]
      }
    }

    def head(): V = _cache
    
    def headKey(): K = _keyCache

    def pop(): V = {
      val answer: V = head()
      reload()
      answer
    }

    def close() = {
      is.close()
    }
  }
}

