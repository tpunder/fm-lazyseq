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
import fm.common.{ByteBufferInputStream, Logging, ProgressStats, Resource, Serializer, Snappy, TaskRunner, UncloseableOutputStream}
import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.util.{List,Comparator,ArrayList,PriorityQueue}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object SortedLazySeq {
  def DefaultBufferSizeLimitMB: Int = 100
  def DefaultBufferRecordLimit: Int = 250000
}

/**
 * A LazySeq that sorts the underlying reader using one or more external temp files and then reads from those files using a merge sort.
 * 
 * The sorting is done once on the first invocation of foreach() and then the temp files can be re-read multiple times without having to re-sort.
 */
final class SortedLazySeq[V, K](reader: LazySeq[V], key: V => K, unique: Boolean = false, bufferSizeLimitMB: Int = SortedLazySeq.DefaultBufferSizeLimitMB, bufferRecordLimit: Int = SortedLazySeq.DefaultBufferRecordLimit)(implicit serializer: Serializer[V], ord: Ordering[K]) extends LazySeq[V] with Logging {
  import serializer._
  
  final def foreach[U](f: V => U): Unit = sortedReader.foreach(f)

  // Debug flag to disable deletion of tmp files
  private val deleteTmpFiles: Boolean = true
  
  // How large of a buffer to use for storing records before sorting and writing to a tmp file (does not take into account the size of the key)
  private val bufferSizeLimitBytes: Int = bufferSizeLimitMB * 1024 * 1024
  
  private case class KeyBytesPair(key: K, bytes: Array[Byte])
  
  private lazy val sortedReader: LazySeq[V] = {
    val sortAndSaveTaskRunner = TaskRunner(name="SortedLazySeq-SortAndSave", threads=1, queueSize=1)
    val stats = ProgressStats.forFasterProcesses()
    
    var buffer = Vector.newBuilder[KeyBytesPair]
    var bufferSizeBytes: Int = 0
    var count: Int = 0
    val sortAndSaveFutures = Vector.newBuilder[Future[MappedByteBuffer]]
    
    def flush() {
      // The result needs to be calculated outside of the sortAndSave task
      val result: Vector[KeyBytesPair] = buffer.result
      sortAndSaveFutures += sortAndSaveTaskRunner.submit{ sortAndSave(result) }
      
      // Clear the buffer and counts
      buffer = Vector.newBuilder[KeyBytesPair]
      bufferSizeBytes = 0
      count = 0
    }
    
    reader.foreach{ v: V =>
      val keyBytes = KeyBytesPair(key(v), serialize(v))
      buffer += keyBytes
      bufferSizeBytes += keyBytes.bytes.length
      count += 1
      if(count >= bufferRecordLimit || bufferSizeBytes > bufferSizeLimitBytes) flush()
      stats.increment
    }
    
    if(count > 0) flush()
    
    sortAndSaveTaskRunner.shutdown()
    
    stats.finalStats
    
    val files: Vector[MappedByteBuffer] = sortAndSaveFutures.result.map{ Await.result(_, Duration.Inf) }
    
    if(files.isEmpty) {
      LazySeq.empty
    } else {
      val tmp = new ReadSortedRecords(files)
      if(unique) tmp.uniqueUsing[K](key) else tmp
    }
  }
  
  private def sortAndSave(buffer: Vector[KeyBytesPair]): MappedByteBuffer = {
    val sorted: Vector[KeyBytesPair] = buffer.sortBy{ _.key }
  
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
      sorted.foreach{ keyValue: KeyBytesPair =>
        os.writeInt(keyValue.bytes.length) // 4 bytes - Size of the data
        os.write(keyValue.bytes)           // Actual data
      }
    } finally {
      os.flush()
      os.close()
    }

    // We use a MappedByteBuffer because if we use "new FileInputStream(raf.getFD)" then reading from
    // the FileInputstream updates the position in the RandomAccessFile which is not what we want.
    // The MappedByteBuffer (which we duplicate() before reading) should make us thread-safe for reading
    // by multiple threads and not require us to reset the RandomAccessFile position before re-reading.
    val buf: MappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
        
    raf.close() // Should be save to close out the RandomAccessFile
    buf
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
  final class ReadSortedRecords(files: Seq[MappedByteBuffer]) extends LazySeq[V] with Closeable {
    
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
  private final class BufferedRecordReader(buf: MappedByteBuffer) extends Closeable {    
    private[this] val is: DataInputStream = new DataInputStream(Snappy.newSnappyOrGzipInputStream(new ByteBufferInputStream(buf.duplicate())))
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
        if(-1 == n) {
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

