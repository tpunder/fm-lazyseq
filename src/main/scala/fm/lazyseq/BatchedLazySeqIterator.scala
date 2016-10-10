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

final private class BatchedLazySeqIterator[A](reader: LazySeq[A], batchSize: Int = 32, bufferSize: Int = 0) extends LazySeqIterator[A] {
  private[this] val batchIterator: LazySeqIterator[IndexedSeq[A]] = new BufferedLazySeq[IndexedSeq[A]](reader.grouped(batchSize), bufferSize).iterator
  
  private[this] var it: Iterator[A] = if (batchIterator.hasNext) batchIterator.next.iterator else Iterator.empty
  
  private[this] var hd: A = _
  private[this] var hdDefined: Boolean = false
  
  def hasNext: Boolean = {
    if (!hdDefined) {
      if (!it.hasNext && batchIterator.hasNext) {
        it = batchIterator.next.iterator
      }
      
      if (it.hasNext) {
        hd = it.next
        hdDefined = true
      }
    }
    
    hdDefined
  }
  
  def head: A = if (hasNext) hd else throw new NoSuchElementException("No more elements in iterator")
  
  def next: A = {
    if (!hasNext) throw new NoSuchElementException("No more elements in iterator")

    val res = hd
    hd = null.asInstanceOf[A]
    hdDefined = false
    
    res
  }
  
  def close(): Unit = batchIterator.close()
}