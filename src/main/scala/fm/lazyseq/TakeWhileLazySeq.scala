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

private object TakeWhileLazySeq {
  private val breaks: Breaks = new Breaks
}

final private class TakeWhileLazySeq[A](reader: LazySeq[A], p: A => Boolean) extends LazySeq[A] {
  import TakeWhileLazySeq.breaks._
  
  final def foreach[U](f: A => U) {
    breakable {
      for (x <- reader) {
        if (p(x)) f(x) else break
      }
    }
  }
}