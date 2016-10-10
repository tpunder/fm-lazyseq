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

final private class DropRightLazySeq[A](reader: LazySeq[A], n: Int) extends LazySeq[A] {
  import java.util.{Queue, ArrayDeque}
  
  private[this] val queue: Queue[A] = new ArrayDeque(n)
  
  final def foreach[U](f: A => U) {
    for (x <- reader) {
      if (queue.size() == n) f(queue.poll())
      queue.add(x)
    }
  }
}