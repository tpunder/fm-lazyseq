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

import scala.collection.{GenTraversableOnce, TraversableOnce}
import scala.collection.generic.{CanBuildFrom, Growable, FilterMonadic}
import scala.util.control.Breaks
import java.util.Random
import fm.common.Implicits._
import fm.common.{Resource, Serializer, TaskRunner}

object LazySeq {
  def defaultThreadCount: Int = Runtime.getRuntime().availableProcessors()
  
  /**
   * Wraps a TraversableOnce in a LazySeq
   */
  def wrap[T](self: TraversableOnce[T]): LazySeq[T] = new TraversableOnceLazySeq[T](self)
  
  /**
   * Wrap a foreach method in a LazySeq
   */
  def wrap[T, U](foreach: (T => U) => Unit): LazySeq[T] = new ForeachLazySeq[T](foreach.asInstanceOf[(T => Any) => Unit])
  
  /**
   * Asynchronously build a LazySeq by spinning up a Producer thread and running the given function to produce elements
   */
  def build[T](queueSize: Int)(f: Growable[T] => Unit): LazySeq[T] = new LazySeqBuilder(queueSize).withProducerThread(f).resourceReader
  
  /**
   * Asynchronously build a LazySeq by spinning up a Producer thread and running the given function to produce elements
   */
  def build[T](f: Growable[T] => Unit): LazySeq[T] = new LazySeqBuilder().withProducerThread(f).resourceReader
  
  /**
   * Combine multiple LazySeqs into a single one by sequentially reading each reader
   */
  def apply[T](readers: TraversableOnce[LazySeq[T]]): LazySeq[T] = new MultiLazySeq[T](readers.toIndexedSeq: _*)
  
  /**
   * Combine multiple LazySeqs into a single one by reading the readers in parallel
   */
  def parCombine[T](readers: TraversableOnce[LazySeq[T]], threads: Int = LazySeq.defaultThreadCount, queueSize: Int = LazySeq.defaultThreadCount): LazySeq[T] = {
    new ParallelMultiLazySeq[T](threads = threads, queueSize = queueSize, readers.toIndexedSeq: _*)
  }
  
  /**
   * This is needed to get the return type of map and flatMap to be a LazySeq
   * via implicits and type casting.
   */
  implicit def canBuildFrom[A,B]: CanBuildFrom[LazySeq[A], B, LazySeq[B]] = null.asInstanceOf[CanBuildFrom[LazySeq[A], B, LazySeq[B]]]

  /**
   * An empty resource reader
   */
  def empty[T]: LazySeq[T] = EmptyLazySeq
  
  /**
   * Our own copy of breaks to avoid conflicts with any other breaks:
   * "Calls to break from one instantiation of Breaks will never target breakable objects of some other instantiation."
   */
  private val breaks = new Breaks
  
  sealed abstract class EitherOrBoth[+L, +R] {
    def leftOption: Option[L]
    def rightOption: Option[R]
  }
  
  final case class Left[L](left: L) extends EitherOrBoth[L, Nothing] {
    def leftOption: Option[L] = Some(left)
    def rightOption: Option[Nothing] = None
  }
  
  final case class Right[R](right: R) extends EitherOrBoth[Nothing, R] {
    def leftOption: Option[Nothing] = None
    def rightOption: Option[R] = Some(right)
  }
  
  final case class Both[L, R](left: L, right: R) extends EitherOrBoth[L, R] {
    def leftOption: Option[L] = Some(left)
    def rightOption: Option[R] = Some(right)
  }
}

/**
 * A Non-Strict Traversable meant to be used for reading resources (Files, InputStreams, etc...) that might not fit into 
 * memory and may or may not be re-readable.
 */
trait LazySeq[+A] extends TraversableOnce[A] with FilterMonadic[A, LazySeq[A]] {
  import LazySeq.breaks._
  
  /**
   * This is the method that sub-classes must implement
   */
  def foreach[U](f: A => U): Unit
  
  /**
   * A Parallel foreach
   */
  final def parForeach[U](f: A => U): Unit = parForeach()(f)
    
  /**
   * A Parallel foreach
   */
  final def parForeach[U](threads: Int = LazySeq.defaultThreadCount, inputBuffer: Int = LazySeq.defaultThreadCount)(f: A => U): Unit = {
    val runner: TaskRunner = TaskRunner("RR-parForeach", threads = threads, queueSize = inputBuffer)
    try {
      for(x <- this) runner.execute{ f(x) }
    } finally {
      runner.shutdown(silent = true)
    }
  }
  
  //
  // Extra Useful Methods that aren't part of TraversableOnce/FilterMonadic
  //
  
  final def ++[B >: A](rest: LazySeq[B]): LazySeq[B] = new AppendedLazySeq[A, B](this, rest)
  
  final def filter(p: A => Boolean): LazySeq[A] = withFilter(p)
  
  final def filterNot(p: A => Boolean): LazySeq[A] = withFilter{ !p(_) }
  
  // LazySeqBuilder overrides this
  def head: A = {
    var result: () => A = () => throw new NoSuchElementException
    breakable {
      for(x <- this) {
        result = () => x
        break
      }
    }
    result()
  }
  
  // LazySeqBuilder overrides this
  def headOption: Option[A] = {
    var result: Option[A] = None
    breakable {
      for(x <- this) {
        result = Some(x)
        break
      }
    }
    result
  }
  
  final def flatten[B](implicit asTraversable: A => GenTraversableOnce[B]): LazySeq[B] = flatMap{ a => asTraversable(a) }
  
  final def grouped[B >: A](size: Int): LazySeq[IndexedSeq[B]] = new GroupedLazySeq(this, size)
  
  final def slice(from: Int, until: Int): LazySeq[A] = new SlicedLazySeq(this, from, until)
  final def take(n: Int): LazySeq[A] = if (n == 0) LazySeq.empty else slice(0, n)
  final def drop(n: Int): LazySeq[A] = if (n == 0) this else slice(n, Int.MaxValue)
  
  final def dropRight(n: Int): LazySeq[A] = if (n == 0) this else new DropRightLazySeq(this, n)
  
  final def takeWhile(p: A => Boolean): LazySeq[A] = new TakeWhileLazySeq(this, p)
  final def dropWhile(p: A => Boolean): LazySeq[A] = new DropWhileLazySeq(this, p)
  
  final def zipWithIndex: LazySeq[(A, Int)] = new ZipWithIndexLazySeq(this)
  
  final def sortBy[B >: A, K](serializer: Serializer[B])(f: B => K)(implicit ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f)(serializer, ord)
  final def sortBy[B >: A, K](f: B => K)(implicit serializer: Serializer[B], ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f)
  
  final def sortBy[B >: A, K](serializer: Serializer[B], bufferSizeLimitMB: Int, bufferRecordLimit: Int)(f: B => K)(implicit ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f, bufferSizeLimitMB = bufferSizeLimitMB, bufferRecordLimit = bufferRecordLimit)(serializer, ord)
  final def sortBy[B >: A, K](bufferSizeLimitMB: Int, bufferRecordLimit: Int)(f: B => K)(implicit serializer: Serializer[B], ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f, bufferSizeLimitMB = bufferSizeLimitMB, bufferRecordLimit = bufferRecordLimit)
  
  final def sorted[B >: A](implicit serializer: Serializer[B], ord: Ordering[B]): LazySeq[B] = new SortedLazySeq[B, B](this, (k: B) => k)
  final def sorted[B >: A](bufferSizeLimitMB: Int, bufferRecordLimit: Int)(implicit serializer: Serializer[B], ord: Ordering[B]): LazySeq[B] = new SortedLazySeq[B, B](this, (k: B) => k, bufferSizeLimitMB = bufferSizeLimitMB, bufferRecordLimit = bufferRecordLimit)
  
  final def shuffle[B >: A](random: Random)(implicit serializer: Serializer[B]): LazySeq[B] = new ShuffledLazySeq[B](this, random)(serializer)
  final def shuffle[B >: A](seed: Long)(implicit serializer: Serializer[B]): LazySeq[B] = new ShuffledLazySeq[B](this, new Random(seed))(serializer)
  final def shuffle[B >: A](implicit serializer: Serializer[B]): LazySeq[B] = new ShuffledLazySeq[B](this)(serializer)
  
  final def uniqueSortBy[B >: A, K](serializer: Serializer[B])(f: B => K)(implicit ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f, true)(serializer, ord)
  final def uniqueSortBy[B >: A, K](f: B => K)(implicit serializer: Serializer[B], ord: Ordering[K]): LazySeq[B] = new SortedLazySeq[B, K](this, f, true)
  
  final def uniqueSorted[B >: A](implicit serializer: Serializer[B], ord: Ordering[B]): LazySeq[B] = new SortedLazySeq[B, B](this, (k: B) => k, true)
  
  // NOTE: this assumes records are sorted (or at least dupes are next to each other)
  final def unique: LazySeq[A] = new UniqueLazySeq[A, A](this, (k: A) => k)
  
  // NOTE: this assumes records are sorted (or at least dupes are next to each other)
  final def uniqueUsing[K](f: A => K): LazySeq[A] = new UniqueLazySeq[A, K](this, f)
  
  /** Assert that this reader is in sorted order */
  final def assertSorted[B >: A](implicit ord: Ordering[B]): LazySeq[B] = new AssertSortedReader[B, B](this, unique = false, v => v)(ord)
  
  /** Assert that this reader is in sorted order AND unique */
  final def assertSortedAndUnique[B >: A](implicit ord: Ordering[B]): LazySeq[B] = new AssertSortedReader[B, B](this, unique = true, v => v)(ord)
  
  final def assertSortedBy[K](key: A => K)(implicit ord: Ordering[K]): LazySeq[A] = new AssertSortedReader[A, K](this, unique = false, key)(ord)
  final def assertSortedAndUniqueBy[K](key: A => K)(implicit ord: Ordering[K]): LazySeq[A] = new AssertSortedReader[A, K](this, unique = true, key)(ord)
  
  /**
   * Mostly standard group by implementation that uses tmp files to store the values of the HashMap
   */
  final def groupBy[B >: A, K](serializer: Serializer[B])(f: A => K): Map[K, LazySeq[B]] = groupBy[B, K](f)(serializer)
  
  final def groupBy[B >: A, K](f: A => K)(implicit serializer: Serializer[B]): Map[K, LazySeq[B]] = {
    import scala.collection.immutable.HashMap
    
    var map = new HashMap[K, TmpFileLazySeqBuilder[B]]
    
    foreach { a: A =>
      val key: K = f(a)
      if(!map.contains(key)) map.updated(key, new TmpFileLazySeqBuilder)
      map(key) += a
    }
    
    map.mapValuesStrict{ _.result }
  }
  
  /**
   * A cross between grouped and groupBy that allows you to specify a key to be
   * used (like in groupBy) instead of a fixed count (like in grouped).  All elements
   * next to each other with the same key get returned in each group.
   * 
   * e.g. LazySeq.wrap(Seq(1,1,1,2,2,1)).groupedBy{ a => a }.toIndexedSeq => Vector((1,Vector(1, 1, 1)), (2,Vector(2, 2)), (1,Vector(1)))
   */
  final def groupedBy[K](by: A => K): LazySeq[(K, IndexedSeq[A])] = new GroupedByLazySeq(this, by)

  /**
   * Collapse elements with the same key by applying a binary operator.
   * 
   * Should be similar to doing something like:
   *   reader.groupBy(key).values.flatMap{ _.reduce(op) }
   */
  final def sortAndCollapseBy[B >: A, K](serializer: Serializer[B])(key: B => K)(op: (B, B) => B)(implicit ord: Ordering[K]): LazySeq[B] = {
    new CollapsingLazySeq(sortBy(serializer)(key), key, op)
  }
  
  final def sortAndCollapseBy[B >: A, K](key: B => K)(op: (B, B) => B)(implicit serializer: Serializer[B], ord: Ordering[K]): LazySeq[B] = {
    new CollapsingLazySeq(sortBy(key), key, op)
  }
  
  final def sortAndCollapseBy[B >: A, K](serializer: Serializer[B], bufferSizeLimitMB: Int, bufferRecordLimit: Int)(key: B => K)(op: (B, B) => B)(implicit ord: Ordering[K]): LazySeq[B] = {
    new CollapsingLazySeq(sortBy(serializer, bufferSizeLimitMB = bufferSizeLimitMB, bufferRecordLimit = bufferRecordLimit)(key), key, op)
  }
  
  final def sortAndCollapseBy[B >: A, K](bufferSizeLimitMB: Int, bufferRecordLimit: Int)(key: B => K)(op: (B, B) => B)(implicit serializer: Serializer[B], ord: Ordering[K]): LazySeq[B] = {
    new CollapsingLazySeq(sortBy(serializer, bufferSizeLimitMB = bufferSizeLimitMB, bufferRecordLimit = bufferRecordLimit)(key), key, op)
  }
    
  final def collapseBy[B >: A, K](key: B => K)(op: (B, B) => B)(implicit ord: Ordering[K]): LazySeq[B] = new CollapsingLazySeq(this, key, op)
  
  /**
   * Split the LazySeq into num buckets of equal size using a round-robin algorithm
   */
  final def bucketize[B >: A](num: Int)(implicit serializer: Serializer[B]): Vector[LazySeq[B]] = {
    val builders = new Array[TmpFileLazySeqBuilder[B]](num)
    (0 until num).foreach{ i => builders(i) = new TmpFileLazySeqBuilder }
    
    var i: Int = 0
    
    foreach { a: A =>
      builders(i % num) += a
      i += 1
    }
    
    val result = Vector.newBuilder[LazySeq[B]]
    builders.foreach{ b => result += b.result }
    result.result
  }
  
  /**
   * Standard partition implementation using LazySeqs
   */
  final def partition[B >: A](p: A => Boolean)(implicit serializer: Serializer[B]): (LazySeq[B], LazySeq[B]) = {
    val left = new TmpFileLazySeqBuilder(serializer)
    val right = new TmpFileLazySeqBuilder(serializer)
    
    foreach { a => if(p(a)) left += a else right += a }
    
    (left.result, right.result)
  }
  
  /**
   * Creates an asynchronous buffer that spins up a producer thread which feeds data into a BlockingQueue that is read using the resulting LazySeq.
   */
  final def buffered(size: Int = 0): LazySeq[A] = new BufferedLazySeq[A](this, size)
  
  /**
   * Performs a parallel map maintaining ordered output
   */
  final def parMap[B](f: A => B): LazySeq[B] = parMap[B]()(f)
  
  /**
   * Performs a parallel map maintaining ordered output
   */
  final def parMap[B](threads: Int = LazySeq.defaultThreadCount, inputBuffer: Int = LazySeq.defaultThreadCount, resultBuffer: Int = LazySeq.defaultThreadCount * 4)(f: A => B): LazySeq[B] = new ParallelMapLazySeq(this, f, threads = threads, inputBuffer = inputBuffer, resultBuffer = resultBuffer) 
  
  /**
   * Performs a parallel flat map maintaining ordered output
   */
  final def parFlatMap[B](f: A => GenTraversableOnce[B]): LazySeq[B] = parFlatMap[B]()(f)
  
  /**
   * Performs a parallel flat map maintaining ordered output
   */
  final def parFlatMap[B](threads: Int = LazySeq.defaultThreadCount, inputBuffer: Int = LazySeq.defaultThreadCount, resultBuffer: Int = LazySeq.defaultThreadCount * 4)(f: A => GenTraversableOnce[B]): LazySeq[B] = {
    parMap(threads, inputBuffer, resultBuffer)(f).flatten
  }
  
  
  final def mergeCorresponding[B >: A](that: LazySeq[B])(implicit ord: Ordering[B]): LazySeq[LazySeq.EitherOrBoth[B, B]] = new MergeCorrespondingLazySeq(this, that, (v: B) => v, (v: B) => v)(ord)
  
  /**
   * Merge corresponding records from this sorted read with that sorted reader given a method to get a common key that can be compared. 
   */
  final def mergeCorrespondingByKey[R, K](that: LazySeq[R], thisKey: A => K, thatKey: R => K)(implicit ord: Ordering[K]): LazySeq[LazySeq.EitherOrBoth[A, R]] = new MergeCorrespondingLazySeq(this, that, thisKey, thatKey)(ord)
  
  /**
   * Run this function on each item before the foreach function
   * 
   * This is basically a foreach but without forcing evaluation of this LazySeq.
   */
  final def before[U](f: A => U): LazySeq[A] = new BeforeAfterLazySeq[A, U, Unit](this, before = f)
  
  /**
   * Run this function on each item after the foreach function
   * 
   * This is basically a foreach but without forcing evaluation of this LazySeq.
   */
  final def after[U](f: A => U): LazySeq[A] = new BeforeAfterLazySeq[A, Unit, U](this, after = f)
  
  /**
   * Same as before() but takes a Resource (i.e. can use it for something like logging)
   */
  final def beforeWithResource[R, U](resource: Resource[R])(f: (A, R) => U): LazySeq[A] = new BeforeAfterWithResourceLazySeq[A, R, U, Unit](this, resource, before = f)
  
  /**
   * Same as after() but takes a Resource (i.e. can use it for something like logging)
   */
  final def afterWithResource[R, U](resource: Resource[R])(f: (A, R) => U): LazySeq[A] = new BeforeAfterWithResourceLazySeq[A, R, Unit, U](this, resource, after = f)
  
  /**
   * Execute the method on the first element of the LazySeq whenever it is evaluated.
   * 
   * Note: The first element is still call via foreach
   */
  final def onFirst[U](f: A => U): LazySeq[A] = new OnFirstLazySeq(this, f)
  
  /**
   * Same as onFirst except for the last element whenever it is evaluated
   */
  final def onLast[U](f: A => U): LazySeq[A] = new OnLastLazySeq(this, f)
  
  //
  // FilterMonadic Implementation
  //
  final def map[B, That](f: A => B)(implicit bf: CanBuildFrom[LazySeq[A], B, That]): That = new MappedLazySeq(this, f).asInstanceOf[That]
  
  final def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit bf: CanBuildFrom[LazySeq[A], B, That]): That = new FlatMappedLazySeq(this, f).asInstanceOf[That]
  
  final def withFilter(p: A => Boolean): LazySeq[A] = new FilteredLazySeq(this, p)
  
  //
  // TraversableOnce Implementation (copied from TraversableLike)
  //  
  final def copyToArray[B >: A](xs: Array[B], start: Int, len: Int) {
    var i = start
    val end = (start + len) min xs.length
    breakable {
      for (x <- this) {
        if (i >= end) break
        xs(i) = x
        i += 1
      }
    }
  }
  
  final def exists(p: A => Boolean): Boolean = {
    var result = false
    breakable {
      for (x <- this) if (p(x)) { result = true; break }
    }
    result
  }
  
  final def find(p: A => Boolean): Option[A] = {
    var result: Option[A] = None
    breakable {
      for (x <- this) if(p(x)) { result = Some(x); break }
    }
    result
  }
  
  final def forall(p: A => Boolean): Boolean = {
    var result = true
    breakable {
      for (x <- this) if(!p(x)) { result = false; break }
    }
    result
  }
  
  def hasDefiniteSize = true
  
  def isEmpty: Boolean = {
    var result = true
    breakable {
      for (x <- this) { result = false; break }
    }
    result
  }
  
  def isTraversableAgain: Boolean = false
  
  def seq: LazySeq[A] = this

  def toIterator: LazySeqIterator[A] = new BatchedLazySeqIterator(this)
  
  def toIterator(batchSize: Int = 32, bufferSize: Int = 0): LazySeqIterator[A] = new BatchedLazySeqIterator(this, batchSize = batchSize, bufferSize = bufferSize)
  
  def toStream: Stream[A] = toIterator.toStream
  
  def toTraversable: Traversable[A] = toStream
}
