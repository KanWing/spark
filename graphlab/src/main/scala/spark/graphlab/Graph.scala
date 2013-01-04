package spark.graphlab

import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel
import spark.KryoRegistrator
import spark.ClosureCleaner
import spark.RDD

/**
 * Class containing the id and value of a vertex
 */
case class Vertex[VD](val id: Vid, val data: VD, val status: Status = true)

/**
 * Class containing both vertices and the edge data associated with an edge
 */
case class Edge[VD, ED](val source: Vertex[VD], val target: Vertex[VD], val data: ED) {
  def otherVertex(vid: Vid) = { if (source.id == vid) target else source; }
  def vertex(vid: Vid) = { if (source.id == vid) source else target; }
} // end of class edge


case class EdgeRecord[ED](val sourceId: Vid, val targetId: Vid, val data: ED) 

case class VertexReplica[VD](val id: Vid, val data: VD, 
  val isActive: Status) 

case class VertexRecord[VD](val data: VD, val isActive: Status, 
  val pids: Array[Pid]) {
  def replicate(id: Vid) = pids.iterator.map(pid => (pid, VertexReplica(id, data, isActive)))
}



// class KeyedRecord with Serializable { def key : Pid }
// class KeyedPartitioner(val numPartitions: Int = 123) extends spark.Partitioner {
//   def getPartition(key: Any) : Int = 
//     math.abs(key.asInstanceOf[KeyedRecord].key) % numPartitions
//   override def equals(other: Any) = other match {
//     case other: KeyedPartitioner => numPartitions == other.numPartitions
//     case _ => false
//   }
//   override def equals(other: Any) = 
//     other.isInstanceOf[PidVidPartitioner] &&
// }
// case class EdgeRecord[ED](val sourceId: Vid, val targetId: Vid, 
//   val data: ED) extends KeyedRecord with Serializable {
//   def key = sourceId
// }
// case class VertexReplica[VD](val pid: Pid, val id: Vid, val data: VD, 
//   val isActive: Status) with Serializable {
//   def key = pid
// }
// case class VertexRecord[VD](val id: Vid, val data: VD, 
//   val isActive: Status, val pids: Array[Pid]) with Serializable {
//   def replicate() = pids.map(pid => VertexReplica(pid, vid, data, isActive))
//   def key = id
// }



object EdgeDirection extends Enumeration {
  val None = Value("None")
  val In = Value("In")
  val Out =  Value("Out")
  val Both = Value("Both")
}


/**
 * A Graph RDD that supports computation on graphs.
 */
class Graph[VD: Manifest, ED: Manifest](
  val vertices: spark.RDD[(Int, VD)],
  val edges: spark.RDD[(Int, Int, ED)]) {

  /**
   * The number of unique vertices in this graph
   */
  lazy val numVertices = vertices.count().toInt

  /**
   * The number of unique edges in this graph
   */
  lazy val numEdges = edges.count().toInt

  /**
   * Create a cached in memory copy of the graph.
   */
  def cache(): Graph[VD, ED] = {
    new Graph(vertices.cache(), edges.cache())
  }

  /**
   * Execute the synchronous powergraph abstraction.
   *
   * gather: (center_vid, edge) => accumulator
   * Todo: Finish commenting
   */
  def iterateDynamic[A: Manifest](
    gather: (Vid, Edge[VD, ED]) => A,
    merge: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    scatter: (Vid, Edge[VD, ED]) => Boolean,
    niter: Int,
    gatherEdges: EdgeDirection.Value = EdgeDirection.In,
    scatterEdges: EdgeDirection.Value = EdgeDirection.Out) = {

    ClosureCleaner.clean(gather)
    ClosureCleaner.clean(merge)
    ClosureCleaner.clean(apply)
    ClosureCleaner.clean(scatter)

    val sc = edges.context
    val numProcs = 10

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable =
      edges.map {
        case (source, target, data) => {
          // val pid = abs((source, target).hashCode()) % numProcs
          val pid = math.abs(source) % numProcs
          (pid, EdgeRecord(source, target, data) )
        }
      }.partitionBy(new HashPartitioner(numProcs), false).cache()

    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numProcs)

    var vTable = vertices.partitionBy(vTablePartitioner)
      .leftOuterJoin(eTable.flatMap { 
        case (pid, EdgeRecord(source, target, _)) => 
          List((source,pid), (target,pid)) 
        }.groupByKey(vTablePartitioner))
      .mapValues {
        case (vdata, None) => VertexRecord(vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => VertexRecord(vdata, true, pids.toArray)
      }.cache()


    // // TODO: rewrite this with accumulator.
    // val replicationFactor = vid2pid.count().toDouble / vTable.count().toDouble
    // println("Replication Factor: " + replicationFactor)
    // println("Edge table size: " + eTable.count.toInt)

    // Loop until convergence or there are no active vertices
    var iter = 0
    val numActive = sc.accumulator(vTable.count.toInt)
    // var numActive = vTable.filter(_._2._2).count

    while (iter < niter && numActive.value > 0) {
    // while (iter < niter && numActive > 0) {
      // Begin iteration
      println("\n\n==========================================================")
      println("Begin iteration: " + iter)
      println("Active:          " + numActive)

      // Gather Phase =========================================================
      val gatherTable = new GraphShardRDD(vTable, eTable, { edge: Edge[VD, ED] =>
        (if (edge.target.status && (gatherEdges == EdgeDirection.In ||
          gatherEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.target.id, gather(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.status && (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.source.id, gather(edge.source.id, edge)))
        } else List.empty)
      }).reduceByKey(vTablePartitioner, merge)

      // Apply Phase ===========================================================
      // Merge with the gather result
      vTable = vTable.leftOuterJoin(gatherTable).mapPartitions(
        iter => iter.map { // Execute the apply if necessary
          case (vid, (VertexRecord(data, true, pids), Some(accum))) =>
            (vid, VertexRecord(apply(new Vertex(vid, data), accum), true, pids))
          case (vid, (VertexRecord(data, true, pids), None)) =>
            (vid, VertexRecord(apply(new Vertex(vid, data), default), true, pids))
          case (vid, (VertexRecord(data, false, pids), _)) => 
            (vid, VertexRecord(data, false, pids))
        }, preservesPartitioning = true).cache()

      // Scatter Phase =========================================================
      val scatterTable = new GraphShardRDD(vTable, eTable, { edge: Edge[VD, ED] =>
        //var accum = List.empty[(Vid, Boolean)]
        (if (edge.target.status && (scatterEdges == EdgeDirection.In ||
          scatterEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.source.id, scatter(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.status && (scatterEdges == EdgeDirection.Out ||
          scatterEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.target.id, scatter(edge.source.id, edge)))
        } else List.empty)
      }).reduceByKey(vTablePartitioner, _ || _)

      // update active vertices
      numActive.value = 0
      vTable = vTable.leftOuterJoin(scatterTable).mapPartitions( _.map{
        case (vid, (VertexRecord(vdata, _, pids), Some(isActive))) => {
          numActive += (if(isActive) 1 else 0)
          (vid, VertexRecord(vdata, isActive, pids))
        }
        case (vid, (VertexRecord(vdata, _, pids), None)) => 
          (vid, VertexRecord(vdata, false, pids))
      }, preservesPartitioning = true).cache()
      vTable.foreach(i => ())
      //numActive = vTable.filter(_._2._2).count
      
      iter += 1
    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
      eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
  } // End of iterate



/**
   * Execute the synchronous powergraph abstraction.
   *
   * gather: (center_vid, edge) => accumulator
   * Todo: Finish commenting
   */
  def iterateStatic[A: Manifest](
    gather: (Vid, Edge[VD, ED]) => A,
    merge: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    numIter: Int,
    gatherEdges: EdgeDirection.Value = EdgeDirection.In) = {

    ClosureCleaner.clean(gather)
    ClosureCleaner.clean(merge)
    ClosureCleaner.clean(apply)

    val sc = edges.context
    val numProcs = 100

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable =
      edges.map {
        case (source, target, data) => {
          // val pid = abs((source, target).hashCode()) % numProcs
          val pid = math.abs(source.hashCode()) % numProcs
          (pid, EdgeRecord(source, target, data))
        }
      }.partitionBy(new HashPartitioner(numProcs), false).cache()

    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numProcs)

    var vTable = vertices.partitionBy(vTablePartitioner)
      .leftOuterJoin(eTable.flatMap { 
        case (pid, EdgeRecord(source, target, _)) => List((source,pid), (target,pid)) 
        }.groupByKey(vTablePartitioner))
      .mapValues {
        case (vdata, None) => VertexRecord(vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => VertexRecord(vdata, true, pids.toArray)
      }

    // Loop until convergence or there are no active vertices
    var iter = 0
    while (iter < numIter)  {

      // Gather Phase =========================================================
      val gatherTable = new GraphShardRDD(vTable, eTable, { edge: Edge[VD, ED] =>
        (if (edge.target.status && (gatherEdges == EdgeDirection.In ||
          gatherEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.target.id, gather(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.status && (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.source.id, gather(edge.source.id, edge)))
        } else List.empty)
      }).reduceByKey(vTablePartitioner, merge)

      // Apply Phase ===========================================================
      // Merge with the gather result
      vTable = vTable.leftOuterJoin(gatherTable).mapPartitions(
        iter => iter.map { // Execute the apply if necessary
          case (vid, (VertexRecord(data, true, pids), Some(accum))) =>
            (vid, VertexRecord(apply(new Vertex(vid, data), accum), true, pids))
          case (vid, (VertexRecord(data, true, pids), None)) =>
            (vid, VertexRecord(apply(new Vertex(vid, data), default), true, pids))
          case (vid, (VertexRecord(data, false, pids), _)) => 
            (vid, VertexRecord(data, false, pids))
        }, preservesPartitioning = true)  

      // end of iteration
      iter += 1
    }

    // Collapse vreplicas, edges and retuen a new graph
    new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
      eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
  } // End of iterate





} // End of Graph RDD

/**
 * Graph companion object
 */
object Graph {

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: Manifest](sc: SparkContext,
    fname: String, edgeParser: String => ED) = {

    // Parse the edge data table
    val edges = sc.textFile(fname).map(
      line => {
        val source :: target :: tail = line.split("\t").toList
        val edata = edgeParser(tail.mkString("\t"))
        (source.trim.toInt, target.trim.toInt, edata)
      }).cache()

    // Parse the vertex data table
    val vertices = edges.flatMap {
      case (source, target, _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _).cache()

    val graph = new Graph[Int, ED](vertices, edges)

    println("Loaded graph:" +
      "\n\t#edges:    " + graph.numEdges +
      "\n\t#vertices: " + graph.numVertices)
    graph
  }

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def fromURL[ED: Manifest](sc: SparkContext,
    fname: String, edgeParser: String => ED) = {

    println("downloading graph")
    val url = new java.net.URL(fname)
    val content = scala.io.Source.fromInputStream(url.openStream).getLines()
      .map(line => {
        val src :: dst :: body = line.split("\t").toList
        val edata = edgeParser(body.mkString("\t"))
        (src.trim.toInt, dst.trim.toInt, edata)
      }).toArray
    val edges = sc.parallelize(content).cache()
    println("determining vertices")
    // Parse the vertex data table
    val vertices = edges.flatMap {
      case (source, target, _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _).cache()

    val graph = new Graph[Int, ED](vertices, edges)

    println("Loaded graph from url:" +
      "\n\t#edges:    " + graph.numEdges +
      "\n\t#vertices: " + graph.numVertices)
    graph
  }

  /**
   * Construct a simple ball and chain graph.
   */
  def ballAndChain(sc: SparkContext) = {
    val ball = Array((1, 2, 0), (2, 3, 0), (3, 1, 0))
    val chain = Array((2, 4, 0), (4, 5, 0), (5, 6, 0))
    val edges = ball ++ chain
    val vertices = edges.flatMap {
      case (source, target, value) => Array(source, target)
    }.distinct.map(vid => (vid, vid))
    new Graph(sc.parallelize(vertices), sc.parallelize(edges)).cache()
  }

  /**
   * Make k-cycles
   */
  def kCycles(sc: SparkContext, numCycles: Int = 3, size: Int = 3) = {
    // Construct the edges
    val edges = for (i <- 0 until numCycles; j <- 0 until size) yield {
      val offset = i * numCycles
      val source = offset + j
      val target = offset + ((j + 1) % size)
      (source, target, i * numCycles + j)
    }
    // Construct the vertices enumerated with the cycle number
    val vertices = edges.flatMap {
      case (source, target, value) => Array(source, target)
    }.distinct.map(vid => (vid, (vid / numCycles) * numCycles))
    new Graph(sc.parallelize(vertices), sc.parallelize(edges))
  }

} // End of Graph Object


