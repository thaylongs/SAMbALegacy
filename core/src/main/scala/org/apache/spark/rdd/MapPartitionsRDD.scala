/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import br.uff.spark.{DataElement, Task, TransformationType}

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}

import scala.collection.AbstractIterator
import scala.collection.Iterator.empty

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (Task, TaskContext, Int, Iterator[DataElement[T]]) => Iterator[DataElement[U]],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[DataElement[U]] =
    f(this.task, context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

private[spark] class FilterMapPartitionsRDD[T: ClassTag](
  prev: RDD[T],
  f: T => Boolean,
  preservesPartitioning: Boolean = false) extends MapPartitionsRDD[T, T](prev, null, preservesPartitioning) {

  setTransformationType(TransformationType.FILTER)
  val ff = (x: DataElement[T]) => f.apply(x.value)
  var defaultNotPassValue: DataElement[String] = null

  override def checkAndPersistProvenance(): RDD[T] = {
    defaultNotPassValue = DataElement.of("don't-pass: " + task.description, task, task.isIgnored)
    super.checkAndPersistProvenance()
    this
  }

  override def compute(split: Partition, context: TaskContext): Iterator[DataElement[T]] = {
    val input = firstParent[T].iterator(split, context)
    /** Returns an iterator over all the elements of this iterator that satisfy the predicate `p`.
      * The order of the elements is preserved.
      *
      * @param p the predicate used to test values.
      * @return an iterator which produces those values of this iterator which satisfy the predicate `p`.
      * @note Reuse: $consumesAndProducesIterator
      *
      * copied by thaylon from: scala.collection.Iterator def filter(p: A => Boolean): Iterator[A]
      **/
    new AbstractIterator[DataElement[T]] {
      // TODO 2.12 - Make a full-fledged FilterImpl that will reverse sense of p
      private var hd: DataElement[T] = _
      private var hdDefined: Boolean = false

      def hasNext: Boolean = hdDefined || {
        var found = false
        do {
          if (!input.hasNext) return false
          hd = input.next()
          found = ff(hd)
          if (!found) {
            defaultNotPassValue.addDepencencie(hd)
          }
        } while (!found)
        hdDefined = true
        true
      }

      def next() = if (hasNext) {
        hdDefined = false;
        DataElement.of(hd.value, hd, task, task.isIgnored)
      } else empty.next()
    }
  }
}