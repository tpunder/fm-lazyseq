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

import java.util.concurrent.TimeUnit
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TestLazySeqBuilder extends AnyFunSuite with Matchers {
  private trait TestObj
  private case object Foo extends TestObj
  private case object Bar extends TestObj
  private case object Baz extends TestObj
  
  private def newBuilder(queueSize: Int = 1): LazySeqBuilder[TestObj] = new LazySeqBuilder[TestObj](queueSize = queueSize, shutdownJVMOnUncaughtException = false)
  
  test("Single Threaded - Close") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder += Foo
    builder.lazySeq.next() should equal (Foo)
    builder.close()
    builder.lazySeq.hasNext should equal (false)
  }
  
  test("Single Threaded - Abort") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder += Foo
    builder.abort()
  }
  
  test("withProducerThread - Close") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
    }
    
    builder.lazySeq.next() should equal (Foo)
    builder.lazySeq.next() should equal (Bar)
    builder.close()
    builder.lazySeq.hasNext should equal (false)
  }
  
  test("withProducerThread - Abort") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
    }
    
    builder.abort()
  }
  
  test("withConsumerThread - Close") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal (IndexedSeq(Foo, Bar))
    }
    
    builder += Foo
    builder += Bar
    builder.close()
  }
  
  test("withConsumerThread - Abort Clean") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal (IndexedSeq(Foo, Bar))
    }
    
    builder += Foo
    builder += Bar
    builder.abort()
  }
  
  test("withConsumerThread - Abort Unclean") {    
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    @volatile var failed: Boolean = false
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal (IndexedSeq(Foo, Bar))
      failed = true
    }
    
    builder += Foo
    builder += Bar
    builder.abort()
    builder.awaitConsumer()
    
    failed should equal (false)
  }
  
  test("withProducerThread - Exception") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      throw new Throwable("Uncaught Throwable from withProducerThread")
    }
    
    try {
      // This will either work or throw an AbortedException depending on timing.
      builder.lazySeq.toIndexedSeq shouldBe IndexedSeq(Foo, Bar)
    } catch {
      case ex: LazySeqBuilder.AbortedException => // This is also okay
    }
    
  }
  
  test("withConsumerThread - Exception") {    
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    
    builder.withConsumerThread { reader =>
      builder.lazySeq.next() should equal (Foo)
      builder.lazySeq.next() should equal (Bar)
      throw new Throwable("Uncaught Throwable from withConsumerThread")
    }
    
    builder += Foo
    builder += Bar
    
    intercept[LazySeqBuilder.AbortedException] {
      (1 to 1000).foreach { i => 
        builder += Foo
        builder += Bar
        builder += Baz
      }
    }
  }
  
  test("withProducerThread/withConsumerThread - delayed consumer") {    
    val builder: LazySeqBuilder[TestObj] = newBuilder(queueSize = 2)
    builder.withProducerThread { growable =>
      growable += Foo
    }
    
    builder.awaitProducer()
    
    @volatile var result: List[TestObj] = Nil
    
    builder.withConsumerThread{ reader =>
      result = reader.toList
    }
    
    builder.awaitConsumer()
    
    result should equal (List(Foo))
  }
  
  test("LazySeqBuilder foreach with exception") {
    val builder: LazySeqBuilder[TestObj] = newBuilder()
    
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      growable += Baz
    }
    
    intercept[Exception] {
      builder.result().foreach { obj =>
        throw new Exception("foo")
      }
    }
    
    builder.awaitProducer()
  }
  
  test("LazySeqBuilder synchronous queue - exception") {
    val builder: LazySeqBuilder[TestObj] = newBuilder(queueSize = 0)
    
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      growable += Baz
    }
    
    intercept[Exception] {
      builder.result().foreach { obj =>
        throw new Exception("foo")
      }
    }
    
    builder.awaitProducer()
  }

  test("grouped with timeout") {
    val builder: LazySeqBuilder[TestObj] = newBuilder(queueSize = 5)

    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      Thread.sleep(1000)
      growable += Baz
      Thread.sleep(1000)
      growable += Foo
      growable += Bar
      growable += Baz
      growable += Foo
      growable += Bar
      growable += Baz
      Thread.sleep(1000)
    }

    val it: LazySeqIterator[IndexedSeq[TestObj]] = builder.lazySeq.grouped(4, 100, TimeUnit.MILLISECONDS).iterator

    it.hasNext shouldBe true
    it.next() shouldBe Vector(Foo, Bar)
    it.hasNext shouldBe true
    it.next() shouldBe Vector(Baz)
    it.hasNext shouldBe true
    it.next() shouldBe Vector(Foo, Bar, Baz, Foo)
    it.hasNext shouldBe true
    it.next() shouldBe Vector(Bar, Baz)
    it.hasNext shouldBe false
  }

  test("grouped with timeout - exception") {
    val builder: LazySeqBuilder[TestObj] = newBuilder(queueSize = 5)

    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      Thread.sleep(1000)
      growable += Baz
      Thread.sleep(1000)
      growable += Foo
      growable += Bar
      growable += Baz
      growable += Foo
      growable += Bar
      growable += Baz
      throw new Exception("foo")
    }

    val it: LazySeqIterator[IndexedSeq[TestObj]] = builder.lazySeq.grouped(4, 100, TimeUnit.MILLISECONDS).iterator

    // These should succeed
    it.hasNext shouldBe true
    it.next() shouldBe Vector(Foo, Bar)
    it.hasNext shouldBe true
    it.next() shouldBe Vector(Baz)

    // Somewhere in this code block we should end up with the AbortedException depending on how the timing works out
    intercept[LazySeqBuilder.AbortedException] {
      while (it.hasNext) it.next()
    }
  }

  test("grouped with timeout - empty") {
    val builder: LazySeqBuilder[TestObj] = newBuilder(queueSize = 5)

    builder.withProducerThread { growable =>
      // Do nothing
    }

    val it: LazySeqIterator[IndexedSeq[TestObj]] = builder.lazySeq.grouped(4, 100, TimeUnit.MILLISECONDS).iterator

    it.hasNext shouldBe false
    intercept[NoSuchElementException] { it.next() }
    intercept[NoSuchElementException] { it.head }
  }
}