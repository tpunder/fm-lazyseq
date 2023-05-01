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

import fm.common.{BuilderCompat, ByteBufferInputStream, ByteBufferUtil, MultiUseResource, Resource, Serializer, Snappy, TraversableOnce, UncloseableOutputStream}
import java.io.{BufferedOutputStream, DataInput, DataInputStream, DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * A builder that lets us build up a temp file that can be read back as a LazySeq.
 * Useful for methods like groupBy, grouped, partition, etc...
 * 
 * Methods are synchronized so this should be thread-safe now
 */
final class TmpFileLazySeqBuilder[A](deleteTmpFiles: Boolean = true)(implicit serializer: Serializer[A]) extends BuilderCompat[A, LazySeq[A]] {
  def this(serializer: Serializer[A]) = this()(serializer)
  
  private[this] val tmpFile: File = File.createTempFile("TmpFileLazySeqBuilder", ".compressed")
  private[this] val raf: RandomAccessFile = new RandomAccessFile(tmpFile, "rw")
  
  // DO NOT USE File.deleteOnExit() since it uses an append-only LinkedHashSet
  // Instead we use the open & unlink from the file system trick to let the OS
  // clean up for us if we don't call close on the RandomAccessFile ourselves.
  if (deleteTmpFiles) tmpFile.delete()

  @volatile private[this] var done: Boolean = false
  
  // Using the builder pattern we cannot use the SerializerWriter or FileOutputStreamResource so we have to do it manually...
  // TODO: find a better way?
  private[this] var writer: DataOutputStream = null

  // Overridden to add make synchronized to prevent needing to re-synchronize for each += call
  override def addAll(xs: TraversableOnce[A]): this.type = synchronized {
    super.addAll(xs)
    this
  }

  // Overridden to add make synchronized to prevent needing to re-synchronize for each += call
  override def addAll(xs: BuilderCompat.TraversableOnceOrIterableOnce[A]): this.type = synchronized {
    super.addAll(xs)
    this
  }
  
  // This logic has to match up with the SerializerWriter...
  // TODO: find a better way?
  override def addOne(elem: A): this.type = synchronized {
    require(!done, "Already produced result!  Cannot add additional elements!")
    if (null == writer) writer = new DataOutputStream(new BufferedOutputStream(Snappy.newSnappyOrGzipOutputStream(UncloseableOutputStream(new FileOutputStream(raf.getFD)))))
    val bytes: Array[Byte] = serializer.serialize(elem)
    require(bytes.length < Int.MaxValue)
    writer.writeInt(bytes.length)
    writer.write(bytes)
    this
  }
  
  override def result(): LazySeq[A] = synchronized {
    require(!done, "Already produced result!")
    done = true
    if (null == writer) EmptyLazySeq else {
      writer.flush()
      writer.close()
      writer = null
      
      val bufs: Vector[MappedByteBuffer] = ByteBufferUtil.map(raf, FileChannel.MapMode.READ_ONLY)
      raf.close()
      
      val resource: Resource[DataInput] = MultiUseResource{ new DataInputStream(Snappy.newSnappyOrGzipInputStream(ByteBufferInputStream(bufs))) }
      new TmpFileLazySeq[A](resource)(serializer)
    } 
  }
  
  override def clear(): Unit = throw new UnsupportedOperationException()
  
  override protected def finalize(): Unit = {
    raf.close()
  }
}