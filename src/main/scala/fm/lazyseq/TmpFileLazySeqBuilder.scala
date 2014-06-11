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

import fm.common.{ByteBufferInputStream, MultiUseResource, Resource, Serializer, Snappy, UncloseableOutputStream}
import java.io.{DataInput, DataInputStream, DataOutputStream, File, FileOutputStream, BufferedOutputStream, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import scala.collection.mutable.Builder

/**
 * A builder that lets us build up a temp file that can be read back as a LazySeq.
 * Useful for methods like groupBy, grouped, partition, etc...
 */
final class TmpFileLazySeqBuilder[A](deleteTmpFiles: Boolean = true)(implicit serializer: Serializer[A]) extends Builder[A, LazySeq[A]] {
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
  
  // This logic has to match up with the SerializerWriter...
  // TODO: find a better way?
  def +=(elem: A) = {
    require(!done, "Already produced result!  Cannot add additional elements!")
    if(null == writer) writer = new DataOutputStream(new BufferedOutputStream(Snappy.newSnappyOrGzipOutputStream(UncloseableOutputStream(new FileOutputStream(raf.getFD)))))
    val bytes: Array[Byte] = serializer.serialize(elem)
    require(bytes.length < Int.MaxValue)
    writer.writeInt(bytes.length)
    writer.write(bytes)
    this
  }
  
  def result: LazySeq[A] = {
    require(!done, "Already produced result!")
    done = true
    if(null == writer) EmptyLazySeq else {
      writer.flush()
      writer.close()
      writer = null
      
      val buf: MappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length())
      raf.close()
      
      val resource: Resource[DataInput] = MultiUseResource{ new DataInputStream(Snappy.newSnappyOrGzipInputStream(new ByteBufferInputStream(buf.duplicate()))) }
      new TmpFileLazySeq[A](resource)(serializer)
    } 
  }
  
  def clear: Unit = throw new UnsupportedOperationException()
  
  override protected def finalize: Unit = {
    raf.close()
  }
}