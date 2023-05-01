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

import fm.common.ProgressStats
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TestTmpFileLazySeq extends AnyFunSuite with Matchers {
  test("Simple Strings") {
    checkString(Seq())
    checkString(Seq("a"))
    checkString(Seq("a","a"))
    
    checkString(Seq("a","b","c","d","e","f"))
    checkString(Seq("b","f","a","d","c","e"))
    
    checkString(Seq("a","b","c","d","e","f","a","b","c","d","e","f"))
    checkString(Seq("b","f","a","d","c","e","b","f","a","d","c","e"))
  }
  
  test("Simple Numeric") {
    checkLong(Seq(1,2,3,4,5))
    checkLong(Seq(5,4,3,2,1))
    
    checkLong(Seq(1,2,3,4,5,1,2,3,4,5))
    checkLong(Seq(5,4,3,2,1,1,2,3,4,5))
  }
  
  test("Large Numeric") {
    checkLong(Range.Long(0L, 1234567L, 1))
  }
  
  def checkString(it: Iterable[String]): Unit = {
    checkLazySeqString(it)
  }

  def checkLazySeqString(it: Iterable[String]): Unit = {
    val builder = new TmpFileLazySeqBuilder[String]()
    builder ++= it
    
    val reader: LazySeq[String] = builder.result()
    reader.toIndexedSeq should equal(it.toIndexedSeq)
  }
  
  def checkLong(it: Iterable[Long]): Unit = {
    checkLazySeqLong(it)
  }
  
  def checkLazySeqLong(it: Iterable[Long]): Unit = {
    val builder = new TmpFileLazySeqBuilder[Long]()
    builder ++= it
    
    val reader: LazySeq[Long] = builder.result()
    
    def check(): Unit = {
      val stats = new ProgressStats
      (reader.toSeq.view zip it.view).foreach{ case (a,b) => 
        stats.increment()
        assert(a.toLong == b, "Expected "+a+" to equal "+b)
      }
      stats.finalStats()
    }
    
    check()
    check() // It should be re-readable
  }
}