package spark.graphlab

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap


import spark.{Aggregator, HashPartitioner, Logging, RDD, SparkEnv, Split, TaskContext}
import spark.{Dependency, OneToOneDependency, ShuffleDependency}
import spark.SparkContext._



private[spark]
case class GraphShardSplit(idx: Int, eTableSplit: Split) extends Split {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

case class VMapRecord[VD,U](val data: VD, val isActive: Status, var accum: Option[U]) 

private[spark] class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    null)
  with Serializable


class GraphShardRDD[VD, ED, U](
    @transient vTable: spark.RDD[(Vid, VertexRecord[VD])], // vid, data, active
    eTable: spark.RDD[(Pid, EdgeRecord[ED])], // pid, src_vid, dst_vid, data
    edgeFun: Edge[VD, ED] => TraversableOnce[(Vid, U)],
    mergeFun: (U, U) => U
    ) extends RDD[(Vid, U)](eTable.context) with Logging {

  // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
  // the shuffle id so we can use it on the slave.
  @transient val vTableReplicated = vTable.flatMap {
    case (vid, vrec) => vrec.replicate(vid)
  }
  @transient val shuffleDependency = new ShuffleDependency(vTableReplicated, eTable.partitioner.get)
  val shuffleId = shuffleDependency.shuffleId

  @transient
  override val dependencies: List[Dependency[_]] =
    List(new OneToOneDependency(eTable), shuffleDependency)

  @transient
  val splits_ = Array.tabulate[Split](eTable.splits.size) {
    i => new GraphShardSplit(i, eTable.splits(i)): Split
  }

  override def splits = splits_

  override val partitioner = Some(eTable.partitioner.get)

  override def preferredLocations(s: Split) =
    eTable.preferredLocations(s.asInstanceOf[GraphShardSplit].eTableSplit)

  override def compute(s: Split, context: TaskContext): Iterator[(Vid, U)] = {

    println("================================================================")
    println("GraphShardRDD.compute")
    println("================================================================")

    var startTime = System.currentTimeMillis

    val split = s.asInstanceOf[GraphShardSplit]
    // Create the vmap.
    val vmap = new it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap[VMapRecord[VD, U]](1000000) 
   

    //val vmapAgg = new Array[VD](1000000)
    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[Pid, VertexReplica[VD]](shuffleId, split.index).foreach {
      case ( _, vrec ) => vmap.put(vrec.id, new VMapRecord[VD,U](vrec.data, vrec.isActive, Option.empty) )
    }

    println("FetchedSplits:  " + ((System.currentTimeMillis - startTime)/1000.0))
    startTime = System.currentTimeMillis
    eTable.iterator(split.eTableSplit, context).foreach { 
      case(_, EdgeRecord(srcId, dstId, edgeData)) => {
        val src = vmap.get(srcId)
        val dst = vmap.get(dstId)
        val srcVertex = Vertex(srcId, src.data, src.isActive)
        val dstVertex = Vertex(dstId, dst.data, dst.isActive)
        val edge = Edge(srcVertex,dstVertex, edgeData)
        edgeFun(edge).foreach{
          case (vid, accum) if vid == srcId => src.accum match {
            case Some(oldAccum) => src.accum = Some(mergeFun(oldAccum, accum))
            case None => src.accum = Some(accum)
          }
          case (vid, accum) if vid == dstId => dst.accum match {
            case Some(oldAccum) => dst.accum = Some(mergeFun(oldAccum, accum))
            case None => dst.accum = Some(accum)
          }
          case _ => throw new IllegalArgumentException("Invalid edge operation.")
        }
      }
    }

    println("ComputeTime:    " + ((System.currentTimeMillis - startTime)/1000.0))
    vmap.iterator.filter(! _._2.accum.isEmpty ).map{ case (vid, rec) => (vid, rec.accum.get) }
  }
}
