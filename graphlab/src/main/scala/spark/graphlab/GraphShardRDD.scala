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


class GraphShardRDD[VD : ClassManifest, ED : ClassManifest, U : ClassManifest](
    @transient vTable: spark.RDD[(Vid, VertexRecord[VD])],
    eTable: spark.RDD[(Pid, EdgeBlockRecord[ED])],
    edgeFun: ((Edge[VD, ED], VMapRecord[VD,U], VMapRecord[VD,U]) => Unit),
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
    val vmap = new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[VMapRecord[VD, U]](1000000)


    // var startTime = System.currentTimeMillis

    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[Pid, VertexReplica[VD]](shuffleId, split.index).foreach {
      case ( _, vrec ) =>
        vmap.put(vrec.id, new VMapRecord[VD,U](vrec.data, vrec.isActive, default) )
    }


    // println("FetchedSplits:  " + ((System.currentTimeMillis - startTime)/1000.0))
    //    startTime = System.currentTimeMillis

    eTable.iterator(split.eTableSplit, context).foreach {
      // get a block of all the edges with the same Pid
      case(pid, ebr) => {

        val sourceIdArray = ebr.sourceId
        val targetIdArray = ebr.targetId
        val dataArray = ebr.data

        val numEdges = sourceIdArray.length
        var i = 0
        var edge: Edge[VD,ED] = null

        while( i < numEdges ) {
          val srcId = sourceIdArray(i)
          val dstId = targetIdArray(i)
          val edgeData = dataArray(i)
          val srcVmap = vmap.get(srcId)
          val dstVmap = vmap.get(dstId)

          assert(srcVmap != null)
          assert(dstVmap != null)

          if(edge == null) {
            val srcVertex = new Vertex(srcId, srcVmap.data, srcVmap.isActive)
            val dstVertex = new Vertex(dstId, dstVmap.data, dstVmap.isActive)
            edge = new Edge(srcVertex, dstVertex, edgeData)
          } else {
            edge.source.id = srcId;
            edge.source.data = srcVmap.data
            edge.source.isActive = srcVmap.isActive
            edge.target.id = dstId
            edge.target.data = dstVmap.data
            edge.target.isActive = dstVmap.isActive
            edge.data = edgeData
          }
          edgeFun(edge, srcVmap, dstVmap)
          i += 1
        }
      }
    }


    // println("ComputeTime:    " + ((System.currentTimeMillis - startTime)/1000.0))
    vmap.iterator.filter( _._2.hasAccum ).map{ case (vid, rec) => (vid, rec.accum) }
  }
}
