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

/**
 * For LazySeq.groupedBy
 */
final class GroupedByLazySeq[A, K](reader: LazySeq[A], by: A => K) extends LazySeq[(K, IndexedSeq[A])] {  
  def foreach[U](f: Tuple2[K, IndexedSeq[A]] => U) {
    var buf = Vector.newBuilder[A]
    var first: Boolean = true
    var prevKey: K = null.asInstanceOf[K]
    
    for(x <- reader) {
      val currentKey: K = by(x)
      
      if (first) {
        prevKey = currentKey
        first = false
      }
      
      // When the key changes we call the function
      if (currentKey != prevKey) {
        f((prevKey, buf.result))
        buf = Vector.newBuilder[A]
        prevKey = currentKey
      }
      
      buf += x
    }
    
    val last = buf.result
    if (last.nonEmpty) f((prevKey, last))
  }
}