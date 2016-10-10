/*
 * Copyright 2016 Frugal Mechanic (http://frugalmechanic.com)
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
 * 
 * Passing a non-null additionalIncrement function allows you to increment the count
 * by an additional custom amount depending on the item.  For example, items might take up
 * different amounts of memory or cost different amounts to process.  You could
 * pass in a custom additionalIncrement to account for that such that calling
 * LazySeq.grouped might give you IndexedSeqs of differing sizes but you could
 * try to keep the amount of memory or processing time more uniform.
 */
final private class GroupedLazySeq[A](reader: LazySeq[A], size: Int, additionalIncrement: A => Int) extends LazySeq[IndexedSeq[A]] {
  def this(reader: LazySeq[A], size: Int) = this(reader, size, null)
  
  assert(size > 0, "size must be > 0 for GroupedLazySeq")
  
  def foreach[U](f: IndexedSeq[A] => U) {
    var count: Int = 0
    var buf = Vector.newBuilder[A]
    
    for (x <- reader) {
      buf += x
      
      val increment: Int = 1 + (if (null == additionalIncrement) 0 else additionalIncrement(x))
      assert(increment > 0, "AdditionalIncrement must be positive: "+increment)
      
      count += increment
      
      if (count >= size) {
        f(buf.result)
        count = 0
        buf = Vector.newBuilder[A]
      }
    }
    
    if (count > 0) f(buf.result)
  }
}