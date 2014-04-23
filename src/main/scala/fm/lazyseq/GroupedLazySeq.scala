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
 * For LazySeq.grouped
 */
final class GroupedLazySeq[A](reader: LazySeq[A], size: Int) extends LazySeq[IndexedSeq[A]] {
  assert(size > 0, "size must be > 0 for GroupedLazySeq")
  
  def foreach[U](f: IndexedSeq[A] => U) {
    var count = 0
    var buf = Vector.newBuilder[A]
    
    for(x <- reader) {
      buf += x
      count += 1
      
      if(count >= size) {
        f(buf.result)
        count = 0
        buf = Vector.newBuilder[A]
      }
    }
    
    if(count > 0) f(buf.result)
  }
}