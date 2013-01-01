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


private[spark] class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    null)
  with Serializable


class GraphShardRDD[VD, ED, U](
    @transient vTable: spark.RDD[(Vid, (VD, Status))], // vid, data, active
    @transient vid2pid: spark.RDD[(Vid, Pid)],
    eTable: spark.RDD[(Pid, (Vid, Vid, ED))], // pid, src_vid, dst_vid, data
    f: Edge[VD, ED] => TraversableOnce[(Vid, U)])
  extends RDD[(Vid, U)](eTable.context) with Logging {

  // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
  // the shuffle id so we can use it on the slave.
  @transient val vTableReplicated = vTable.join(vid2pid).map {
    case (vid, ((vdata, active), pid)) => (pid, (vid, (vdata, active)))
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

    val split = s.asInstanceOf[GraphShardSplit]
    // Create the vmap.
    val vmap = new JHashMap[Vid, (VD, Status)]
    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[Pid, (Vid, (VD, Status))](shuffleId, split.index).foreach {
      case ( key: Pid, value:  (Vid, (VD, Status)) ) => vmap.put(value._1, value._2)
    }

    eTable.iterator(split.eTableSplit, context).flatMap { case(_, (srcId, dstId, edgeData)) =>
      val (srcData, srcStatus)  = vmap.get(srcId)
      val (dstData, dstStatus) = vmap.get(dstId)
      f(Edge(Vertex(srcId, srcData, srcStatus), Vertex(dstId, dstData, dstStatus), edgeData))
    }
  }
}
