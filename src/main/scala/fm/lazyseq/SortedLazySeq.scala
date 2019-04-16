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

import fm.common.Serializer

/**
 * A LazySeq that sorts the underlying reader using one or more external temp files and then reads from those files using a merge sort.
 * 
 * The sorting is done once on the first invocation of foreach() and then the temp files can be re-read multiple times without having to re-sort.
 */
final private class SortedLazySeq[V, K](
  reader: LazySeq[V],
  key: V => K,
  unique: Boolean = false,
  bufferSizeLimitMB: Int = SortedLazySeqBuilder.DefaultBufferSizeLimitMB,
  bufferRecordLimit: Int = SortedLazySeqBuilder.DefaultBufferRecordLimit
)(implicit serializer: Serializer[V], ord: Ordering[K]) extends LazySeq[V] {

  final def foreach[U](f: V => U): Unit = sortedReader.foreach(f)

  private lazy val sortedReader: LazySeq[V] = {
    val builder: SortedLazySeqBuilder[V, K] = new SortedLazySeqBuilder(
      key = key,
      unique = unique,
      bufferSizeLimitMB = bufferSizeLimitMB,
      bufferRecordLimit = bufferRecordLimit
    )

    builder ++= reader
    builder.result
  }
}

