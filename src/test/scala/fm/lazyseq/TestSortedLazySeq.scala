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

import org.scalatest.FunSuite
import org.scalatest.Matchers
import fm.common.ProgressStats

final class TestSortedLazySeq extends FunSuite with Matchers {
  test("Simple String Sorting") {
    checkStringSort(Seq(), Seq())
    checkStringSort(Seq("a"), Seq("a"))
    checkStringSort(Seq("a","a"), Seq("a","a"))
    
    checkStringSort(Seq("a","b","c","d","e","f"), Seq("a","b","c","d","e","f"))
    checkStringSort(Seq("b","f","a","d","c","e"), Seq("a","b","c","d","e","f"))
    
    checkStringSort(Seq("a","b","c","d","e","f","a","b","c","d","e","f"), Seq("a","a","b","b","c","c","d","d","e","e","f","f"))
    checkStringSort(Seq("b","f","a","d","c","e","b","f","a","d","c","e"), Seq("a","a","b","b","c","c","d","d","e","e","f","f"))
  }
  
  test("Simple Unique String Sorting") {
    checkStringSort(Seq(), Seq(), true)
    checkStringSort(Seq("a"), Seq("a"), true)
    checkStringSort(Seq("a","a"), Seq("a"), true)
    
    checkStringSort(Seq("a","a","b","b","c","c","d","d","e","e","e","f","f","f"), Seq("a","b","c","d","e","f"), true)
    checkStringSort(Seq("a","b","c","d","e","f","a","b","c","d","e","f"), Seq("a","b","c","d","e","f"), true)
  }
  
  test("Simple Numeric Sorting") {
    checkLongSort(Seq(1,2,3,4,5), Seq(1,2,3,4,5))
    checkLongSort(Seq(5,4,3,2,1), Seq(1,2,3,4,5))
    
    checkLongSort(Seq(1,2,3,4,5,1,2,3,4,5), Seq(1,1,2,2,3,3,4,4,5,5))
    checkLongSort(Seq(5,4,3,2,1,1,2,3,4,5), Seq(1,1,2,2,3,3,4,4,5,5))
  }
  
  test("Simple Unique Numeric Sorting") {
    checkLongSort(Seq(1,1,2,3,4,5,5,1,2,3,4,5,1,2,3,4,5), Seq(1,2,3,4,5), true)
    checkLongSort(Seq(5,4,3,2,1,1,2,3,4,5), Seq(1,2,3,4,5), true)
  }
  
  test("Large Numeric Sorting") {
    checkLongSort(Range.Long(0L, 1234567L, 1), Range.Long(0L, 1234567L, 1))
  }
  
  test("Large Unique Numeric Sorting") {
    checkLongSort(Range.Long(0L, 1234567L, 1)++Range.Long(0L, 1234567L, 1), Range.Long(0L, 1234567L, 1), true)
  }
  
  def checkStringSort(it: Iterable[String], result: Iterable[String], unique: Boolean = false) {
    checkLazySeqStringSort(it, result, unique)
  }

  def checkLazySeqStringSort(it: Iterable[String], result: Iterable[String], unique: Boolean = false) {
    val reader = if(unique) LazySeq.wrap(it).uniqueSorted else LazySeq.wrap(it).sorted
    reader.toIndexedSeq should equal(result.toIndexedSeq)
  }
  
  def checkLongSort(it: Iterable[Long], result: Iterable[Long], unique: Boolean = false) {
    checkLazySeqLongSort(it, result, unique)
  }
  
  def checkLazySeqLongSort(it: Iterable[Long], result: Iterable[Long], unique: Boolean = false) {
    val reader: LazySeq[Long] = if(unique) LazySeq.wrap(it).uniqueSorted else LazySeq.wrap(it).sorted
    
    def check(): Unit = {
      val stats = new ProgressStats
      (reader.toSeq.view zip result.view).foreach{ case (a,b) => 
        stats.increment
        assert(a.toLong == b, "Expected "+a+" to equal "+b)
      }
      stats.finalStats
    }
    
    check()
    check() // It should be re-readable
  }
}