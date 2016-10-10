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
 * For LazySeq.beforeWithResource and LazySeq.afterWithResource
 */
final private class BeforeAfterWithResourceLazySeq[A, R, BEFORE, AFTER](reader: LazySeq[A], resource: Resource[R], before: (A, R) => BEFORE = (a: A, r: R) => {}, after: (A, R) => AFTER = (a: A, r: R) => {}) extends LazySeq[A] {
  def foreach[U](f: A => U): Unit = resource.use { r =>
    reader.foreach { a =>
      before(a, r)
      val res = f(a)
      after(a, r)
      res
    }
  }
}