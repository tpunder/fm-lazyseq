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

import fm.common.TaskRunner

/**
 * Like the MultiLazySeq but will read from the readers in parallel
 */
final class ParallelMultiLazySeq[A](threads: Int, queueSize: Int, readers: LazySeq[A]*) extends LazySeq[A] {
  import java.util.concurrent.ArrayBlockingQueue
  
  // For marking the end of an individual reader
  private[this] val END_OF_READER: AnyRef = new Object{}
  
  final def foreach[U](f: A => U): Unit = {
    val taskRunner: TaskRunner = TaskRunner("ParallelMulti-RR", threads = threads, queueSize = readers.size)
    val queue: ArrayBlockingQueue[AnyRef] = new ArrayBlockingQueue(queueSize)
    val batchSize: Int = 8
    
    try {
      // The taskRunner queue is sized to be large enough to hold all of the tasks
      // so this section of code doesn't block
      for (reader <- readers) taskRunner.execute {
        reader.grouped(batchSize).foreach{ v: IndexedSeq[A] => queue.put(v.asInstanceOf[AnyRef]) }
        queue.put(END_OF_READER)
      }
  
      var counter: Int = readers.size
      
      while(counter > 0) {
        val elem: AnyRef = queue.take()
        if (elem eq END_OF_READER) counter -= 1 else elem.asInstanceOf[IndexedSeq[A]].foreach{ f }
      }
      
      taskRunner.shutdown(silent = true)
    } catch {
      case ex: Throwable =>
        taskRunner.abort(5)
        throw ex
    }
  }
}