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

import fm.common.Resource

/**
 * For LazySeq.mergeCorresponding
 */
final private class MergeCorrespondingLazySeq[L, R, K](leftReader: LazySeq[L], rightReader: LazySeq[R], toLeftKey: L => K, toRightKey: R => K)(implicit ord: Ordering[K]) extends LazySeq[LazySeq.EitherOrBoth[L, R]] {
  private[this] val asserts: Boolean = true
  
  import LazySeq.{EitherOrBoth, Left, Right, Both}
  
  // This uses a thread for the rightReader to create the iterator
  final def foreach[U](f: EitherOrBoth[L, R] => U) = Resource(rightReader.toIterator()).use { rightIt =>
    
    var prevLeftKey: K = null.asInstanceOf[K]
    var prevLeftKeyDefined: Boolean = false
    
    var prevRightKey: K = null.asInstanceOf[K]
    var prevRightKeyDefined: Boolean = false
    
    leftReader.foreach { left: L =>
      if (asserts) {
        val leftKey: K = toLeftKey(left)
        if (prevLeftKeyDefined) assert(ord.lt(prevLeftKey, leftKey), "Incorrect usage of MergeCorrespondingLazySeq.  Inputs are not sorted/unique!")
        else prevLeftKeyDefined = true
        prevLeftKey = leftKey
      }
      
      // If nothing left on the right then we use the left value
      if (!rightIt.hasNext) f(Left(left))
      else {
        val leftKey: K = toLeftKey(left)
        
        // Empty out any right side keys that are less than the current left key
        while (rightIt.hasNext && ord.lt(toRightKey(rightIt.head), leftKey)) {
          val right: R = rightIt.next
          
          if (asserts) {
            val rightKey: K = toRightKey(right)
            if (prevRightKeyDefined) assert(ord.lt(prevRightKey, rightKey), "Incorrect usage of MergeCorrespondingLazySeq.  Inputs are not sorted/unique!")
            else prevRightKeyDefined = true
            prevRightKey = rightKey
          }
          
          f(Right(right))
        }

        // Either the keys match and we return a Both OR there are either no remaining right values
        // or the right key is greater than the left key
        if (rightIt.hasNext && ord.equiv(leftKey, toRightKey(rightIt.head))) {
          val right: R = rightIt.next
          
          if (asserts) {
            val rightKey: K = toRightKey(right)
            if (prevRightKeyDefined) assert(ord.lt(prevRightKey, rightKey), "Incorrect usage of MergeCorrespondingLazySeq.  Inputs are not sorted/unique!")
            else prevRightKeyDefined = true
            prevRightKey = rightKey
          }
          
          f(Both(left, right))
        }
        else f(Left(left)) // No remaining right values OR the right key is greater than the left key

      }
    }
    
    // Drain anything left over on the right side
    while (rightIt.hasNext) {
      val right: R = rightIt.next
      
      if (asserts) {
        val rightKey: K = toRightKey(right)
        if (prevRightKeyDefined) assert(ord.lt(prevRightKey, rightKey), "Incorrect usage of MergeCorrespondingLazySeq.  Inputs are not sorted/unique!")
        else prevRightKeyDefined = true
        prevRightKey = rightKey
      }
      
      f(Right(right))
    }
  }
}