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

import java.io.{DataInput, EOFException, File}
import java.nio.ByteBuffer
import fm.common.{InputStreamResource, Logging, Resource, Serializer}

object SerializerReader {
  def apply[A](file: File, serializer: Serializer[A]): SerializerReader[A] = {
    SerializerReader(InputStreamResource.forFileOrResource(file).dataInput(), serializer)
  }
  
  /**
   * Use the File file name information and the ByteBuffer (which is probably a MappedByteBuffer) to do the 
   * actual reading since the File might not exist on the file system
   */
  def apply[A](file: File, buf: ByteBuffer, serializer: Serializer[A]): SerializerReader[A] = {
    SerializerReader(InputStreamResource.forByteBuffer(buf, originalFileName = file.getName()).dataInput(), serializer)
  }
}

final case class SerializerReader[A](resource: Resource[DataInput], serializer: Serializer[A]) extends LazySeq[A] with Logging {
  
  import scala.util.control.Breaks
  
  private[this] val breaks = new Breaks 
  import breaks.{break, breakable}
  
  def foreach[U](f: A => U): Unit = {
    resource.use{ input: DataInput =>
      
      breakable {
        while(true) {
          val size: Int = try { input.readInt() } catch { case ex: EOFException => break }
          assert(size > 0)
          val bytes: Array[Byte] = new Array[Byte](size)
          input.readFully(bytes)
          f(serializer.deserialize(bytes))
        }
      }
    }
  }
}
