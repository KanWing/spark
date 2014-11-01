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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext, SparkContext}



class LoopIterator[T](generator: () => Iterator[T]) extends Iterator [T] {
  var iter = generator()
  override def hasNext() = iter.hasNext
  override def next(): T = iter.next()
  def reset() { iter = generator() }
  def nextLooped(): T = {
    // reset the iterator if necessary
    if (!iter.hasNext) { iter = generator() }
    iter.next()
  }
}


private[spark] class MapPartitionsLoopedRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: LoopIterator[T] => Iterator[U])
  extends RDD[U](prev) {

  override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    f(new LoopIterator( () => firstParent[T].iterator(split, context) ))
}


private[spark] class ZippedPartitionsLoopedRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    f: (LoopIterator[A], LoopIterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(new LoopIterator(() => rdd1.iterator(partitions(0), context)), 
      new LoopIterator(() => rdd2.iterator(partitions(1), context)))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
