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
 * Used for LazySeq.zipWithIndex
 */
final private class ZipWithIndexLazySeq[A](reader: LazySeq[A]) extends LazySeq[(A, Int)] {
  final def foreach[U](f: Tuple2[A, Int] => U) {
    var i: Int = 0
    for (x <- reader) {
      f((x, i))
      i += 1
    }
  }
}