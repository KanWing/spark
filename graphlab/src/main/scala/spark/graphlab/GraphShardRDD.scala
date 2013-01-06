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

case class VMapRecord[
  @specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD,
  @specialized(Char, Int, Boolean, Byte, Long, Float, Double)U](
  val data: VD, val isActive: Status, var accum: U, var hasAccum: Boolean = false) {
  def add(other: U, mergeFun: (U,U) => U ) {
    if(hasAccum) accum = mergeFun(accum, other)
    else { accum = other; hasAccum = true }
  }
}

private[spark] class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    null)
  with Serializable


class GraphShardRDD[VD, ED, U](
    @transient vTable: spark.RDD[(Vid, VertexRecord[VD])], // vid, data, active
    eTable: spark.RDD[(Pid, EdgeBlockRecord[ED])], // pid, src_vid, dst_vid, data
    edgeFun: Edge[VD, ED] => TraversableOnce[(Vid, U)],
    mergeFun: (U, U) => U,
    default: U
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

    val split = s.asInstanceOf[GraphShardSplit]
    // Create the vmap.
    val vmap = new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[VMapRecord[VD, U]](1000000)

    var startTime = System.currentTimeMillis

    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[Pid, VertexReplica[VD]](shuffleId, split.index).foreach {
      case ( _, vrec ) =>
      vmap.put(vrec.id, new VMapRecord[VD,U](vrec.data, vrec.isActive, default) )
    }

    // println("FetchedSplits:  " + ((System.currentTimeMillis - startTime)/1000.0))
    startTime = System.currentTimeMillis
    eTable.iterator(split.eTableSplit, context).foreach {
      // get a block of all the edges with the same Pid
      case(_, EdgeBlockRecord(sourceIdArray, targetIdArray , dataArray)) => {
        val numEdges = sourceIdArray.length
        var i = 0
        while( i < numEdges ) {
          val srcId = sourceIdArray(i)
          val dstId = targetIdArray(i)
          val edgeData = dataArray(i)
          val src = vmap.get(srcId)
          val dst = vmap.get(dstId)
          val srcVertex = Vertex(srcId, src.data, src.isActive)
          val dstVertex = Vertex(dstId, dst.data, dst.isActive)
          val edge = Edge(srcVertex,dstVertex, edgeData)
          edgeFun(edge).foreach{
            case (vid, accum) =>
              if(vid == srcId) src.add(accum, mergeFun)
              else if(vid == dstId) dst.add(accum, mergeFun)
              else throw new IllegalArgumentException("Invalid edge operation.")
          }
          i += 1
        }
      }
    }
    // println("ComputeTime:    " + ((System.currentTimeMillis - startTime)/1000.0))
    vmap.iterator.filter( _._2.hasAccum ).map{ case (vid, rec) => (vid, rec.accum) }
  }
}
