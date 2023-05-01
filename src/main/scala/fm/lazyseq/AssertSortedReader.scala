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

final private class AssertSortedReader[A, K](reader: LazySeq[A], unique: Boolean, toKey: A => K)(implicit ord: Ordering[K]) extends LazySeq[A] {
  final def foreach[U](f: A => U): Unit = {
    var prevKey: K = null.asInstanceOf[K]
    var prevKeyDefined: Boolean = false
    
    reader.foreach { (elem: A) =>
      val key: K = toKey(elem)
      
      if (prevKeyDefined) {        
        if (unique) assert(ord.lt(prevKey, key), s"Reader is not sorted/Unique!  Prev Key: $prevKey  Current Key: $key")
        else assert(ord.lteq(prevKey, key), s"Reader is not sorted!  Prev Key: $prevKey  Current Key: $key")
      } else {
        prevKeyDefined = true
      }
      
      prevKey = key
      
      f(elem)
    }
  }
}