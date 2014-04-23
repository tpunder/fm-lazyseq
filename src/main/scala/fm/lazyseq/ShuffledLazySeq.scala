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

import java.util.Random
import fm.common.Serializer

/**
 * Randomly Shuffles the records 
 */
final class ShuffledLazySeq[V](reader: LazySeq[V], random: Random = new Random())(implicit serializer: Serializer[V]) extends LazySeq[V] {
  import java.nio.ByteBuffer

  private case class KeyValue(key: Long, value: V)
  
  private object KeyValueSerializer extends Serializer[KeyValue] {
    def serialize(value: KeyValue): Array[Byte] = ByteBuffer.allocate(8).putLong(value.key).array() ++ serializer.serialize(value.value)
    def deserialize(bytes: Array[Byte]): KeyValue = {
      val (keyBytes, valueBytes) = bytes.splitAt(8) 
      KeyValue(ByteBuffer.wrap(keyBytes).getLong, serializer.deserialize(valueBytes))
    }
  } 
  
  private def shuffledReader = reader.map{ v => KeyValue(random.nextLong(), v) }.sortBy(KeyValueSerializer){ _.key }.map{ _.value }
  
  final def foreach[U](f: V => U): Unit = shuffledReader.foreach(f)
}