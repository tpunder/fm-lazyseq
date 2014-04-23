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

import scala.util.control.Breaks

/**
 * Used for LazySeq.slice
 */
object SlicedLazySeq {
  private val breaks = new Breaks
}

final class SlicedLazySeq[A](reader: LazySeq[A], from: Int, until: Int) extends LazySeq[A] {
  require(from < until, s"Expected from < until but got from: $from  until: $until")
  
  import SlicedLazySeq.breaks._
  
  final def foreach[U](f: A => U) {
    if(from < 0) throw new IllegalArgumentException("Slice 'from' argument cannot be < 0")
    
    var i = 0
    breakable {
      for (x <- reader) {
        if(i >= from) f(x)
        i += 1
        if(i >= until) break
      }
    }
  }
}