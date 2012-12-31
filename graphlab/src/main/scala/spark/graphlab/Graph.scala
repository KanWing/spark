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
case class Vertex[VD](val id: Vid, val data: VD)

/**
 * Class containing both vertices and the edge data associated with an edge
 */
case class Edge[VD, ED](val source: Vertex[VD], val target: Vertex[VD],
  val data: ED) {
  def otherVertex(vid: Vid) = { if (source.id == vid) target else source; }
  def vertex(vid: Vid) = { if (source.id == vid) source else target; }
} // end of class edge

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
   * The join edges and vertices function returns a table which joins
   * the vertex and edge data.
   */
  def groupEdgesAndVertices(
    vTable: spark.RDD[(Vid, (VD, Boolean))], // vid, data, active
    eTable: spark.RDD[(Pid, (Vid, Vid, ED))], // pid, src_vid, dst_vid, data
    vid2pid: spark.RDD[(Vid, Pid)]) = {
    // Split the table among machines
    vTable.join(vid2pid).map {
      case (vid, ((vdata, active), pid)) => (pid, (vid, (vdata, active)))
    }.cogroup(eTable)
  } // end of join edges and vertices

  /**
   * Execute the synchronous powergraph abstraction.
   *
   * gather: (center_vid, edge) => accumulator
   * Todo: Finish commenting
   */
  def iterate[A: Manifest](
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

    val numProcs = 100

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable =
      edges.map {
        case (source, target, data) => {
          // val pid = abs((source, target).hashCode()) % numProcs
          val pid = math.abs(source.hashCode()) % numProcs
          (pid, (source, target, data))
        }
      }.cache()

    // The master vertices are used during the apply phase
    var vTable = vertices.map { case (vid, vdata) => (vid, (vdata, true)) }.cache()

    // Create a map from vertex id to the partitions that contain that vertex
    // (vid, pid)
    val vid2pid = eTable.flatMap {
      case (pid, (source, target, _)) => List((source, pid), (target, pid))
    }.distinct().cache() // what is the role of the number of bins?

    val replicationFactor = vid2pid.count().toDouble / vTable.count().toDouble
    println("Replication Factor: " + replicationFactor)
    println("Edge table size: " + eTable.count.toInt)

    // Loop until convergence or there are no active vertices
    var iter = 0
    var numActive = vTable.filter(_._2._2).count

    while (iter < niter && numActive > 0) {
      // Begin iteration
      println("\n\n==========================================================")
      println("Begin iteration: " + iter)
      println("Active:          " + numActive)

      iter += 1

      // Gather Phase ---------------------------------------------
      val gatherTable = groupEdgesAndVertices(vTable, eTable, vid2pid).flatMap {
        case (pid, (vTable, eTable)) => {
          println("Populating gather map")
          // Create an in memory map representation of all the vertices
          val vMap = vTable.toMap
          println("processsing gather edges")
          // Iterate over edges
          val result = eTable.flatMap {
            case (srcId, dstId, eData) => {
              val (srcData, srcActive) = vMap(srcId)
              val (dstData, dstActive) = vMap(dstId)
              val edge = new Edge(Vertex(srcId, srcData), new Vertex(dstId, dstData), eData)
              var accum = List.empty[(Vid, A)]
              if (dstActive && (gatherEdges == EdgeDirection.In ||
                gatherEdges == EdgeDirection.Both)) { // gather on the target
                accum ++= List((dstId, gather(dstId, edge)))
              }
              if (srcActive && (gatherEdges == EdgeDirection.Out ||
                gatherEdges == EdgeDirection.Both)) { // gather on the source
                accum ++= List((srcId, gather(srcId, edge)))
              }
              accum
            }
          }
          println("finished gather edges")
          result
        }
      }.reduceByKey(merge)

      // Apply Phase ---------------------------------------------
      // Merge with the gather result
      vTable = vTable.leftOuterJoin(gatherTable).mapPartitions(
        iter => iter.map { // Execute the apply if necessary
          case (vid, ((data, true), Some(accum))) =>
            (vid, (apply(new Vertex[VD](vid, data), accum), true))
          case (vid, ((data, true), None)) =>
            (vid, (apply(new Vertex[VD](vid, data), default), true))
          case (vid, ((data, false), _)) => (vid, (data, false))
        }, true).cache()

      // Scatter Phase ---------------------------------------------
      val scatterTable = groupEdgesAndVertices(vTable, eTable, vid2pid).flatMap {
        case (pid, (vTable, eTable)) => {
          println("building scatter map")
          // Create an in memory map representation of all the vertices
          val vMap = vTable.toMap
          println("processsing scatter edges")
          // Iterate over edges
          val result = eTable.flatMap {
            case (srcId, dstId, eData) => {
              val (srcData, srcActive) = vMap(srcId)
              val (dstData, dstActive) = vMap(dstId)
              val edge = new Edge(Vertex(srcId, srcData), new Vertex(dstId, dstData), eData)
              var accum = List.empty[(Vid, Status)]
              if (dstActive && (scatterEdges == EdgeDirection.In ||
                scatterEdges == EdgeDirection.Both)) { // gather on the target
                accum ++= List((srcId, scatter(dstId, edge)))
              }
              if (srcActive && (scatterEdges == EdgeDirection.Out ||
                scatterEdges == EdgeDirection.Both)) { // gather on the source
                accum ++= List((dstId, scatter(srcId, edge)))
              }
              accum
            }
          }
          println("Finished scatter edges")
          result
        }
      }.reduceByKey(_ || _)

      // update active vertices
      vTable = vTable.leftOuterJoin(scatterTable).map {
        case (vid, ((vdata, _), Some(new_active))) => (vid, (vdata, new_active))
        case (vid, ((vdata, _), None)) => (vid, (vdata, false))
      }.cache()

      // Compute the number active
      numActive = vTable.map {
        case (_, (_, active)) => if (active) 1 else 0
      }.reduce(_ + _);

    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    new Graph(vTable.map { case (vid, (vdata, _)) => (vid, vdata) },
      eTable.map { case (pid, (source, target, edata)) => (source, target, edata) })
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


