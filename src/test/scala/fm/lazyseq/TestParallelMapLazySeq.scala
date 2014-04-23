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

final class TestParallelMapLazySeq extends FunSuite with Matchers {
  private trait TestObj
  private case object Foo extends TestObj
  private case object Bar extends TestObj
  private case object Baz extends TestObj
  
  private case class MapException() extends Exception("This is an expected exception")
  private case class ForeachException() extends Exception("This is an expected exception")
  
  private def newReader[B](map: TestObj => B): ParallelMapLazySeq[TestObj, B] = new ParallelMapLazySeq(LazySeq.wrap(Vector[TestObj](Foo, Bar, Baz)), map)
  private def mapThatThrows(obj: TestObj): Nothing = throw new MapException
  private def identMap(obj: TestObj): TestObj = obj
  
  test("Map Throws Exception That should be propagated to original thread") {
    val reader = newReader(mapThatThrows)
    intercept[MapException]{ reader.foreach{ obj => throw new ForeachException } }
  }
  
  test("Foreach throws Exception that should be propagated to original thread") {
    val reader = newReader(identMap)
    intercept[ForeachException]{ reader.foreach{ obj => throw new ForeachException } }
  }
}