package spark.graphlab

import scala.math._
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
  val Out = Value("Out")
  val Both = Value("Both")
}

/**
 * A partitioner for tuples that only examines the first value in the tuple.
 */
class PidVidPartitioner(val numPartitions: Int = 4) extends spark.Partitioner {
  def getPartition(key: Any): Int = key match {
    case (pid: Pid, vid: Vid) => math.abs(pid) % numPartitions
    case _ => 0
  }
  override def equals(other: Any) = other match {
    case other: PidVidPartitioner => numPartitions == other.numPartitions
    case _ => false
  }
}

/**
 * A Graph RDD that supports computation on graphs.
 */
class Graph[VD: Manifest, ED: Manifest](
  val vertices: spark.RDD[(Int, VD)],
  val edges: spark.RDD[((Int, Int), ED)]) {

  /**
   * The number of unique vertices in this graph
   */
  lazy val numVertices = vertices.count().toInt

  /**
   * The number of unique edges in this graph
   */
  lazy val numEdges = edges.count().toInt

  /**
   * A partitioner which operates on tuples of (pid,vid) and partitions
   * based on the first
   */
  private val pidVidPartitioner = new PidVidPartitioner()

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
  private def joinEdgesAndVertices(
    vTable: spark.RDD[(Vid, (VD, Boolean))], // vid, data, active
    eTable: spark.RDD[((Pid, Vid), (Vid, ED))], // pid, src_vid, dst_vid, data
    vid2pid: spark.RDD[(Vid, Pid)]) // vid, pid
    : RDD[((Pid, Vid), (Vid, VD, Status, ED, Vid, VD, Status))] = {
    // (pid, vid), (src_vid, src_data, src_active, e_data, dst_vid, dst_data, dst_active)

    // Split the table among machines
    val vreplicas = vTable.join(vid2pid).map {
      case (vid, ((vdata, active), pid)) => ((pid, vid), (vdata, active))
    }.partitionBy(pidVidPartitioner).cache()

    // Generate the large edge table
    val preservePartitioning = true
    eTable
      .join(vreplicas).mapPartitions(iter => iter.map {
        // Join with the source vertex data and rekey with target vertex
        case ((pid, source_id), ((target_id, edata), (source_vdata, source_active))) =>
          ((pid, target_id), (source_id, source_vdata, source_active, edata))
      }, preservePartitioning)
      .join(vreplicas).mapPartitions(iter => iter.map {
        // Join with the target vertex and rekey back to the source vertex
        case ((pid, target_id),
          ((source_id, source_vdata, source_active, edata), (target_vdata, target_active))) =>
          ((pid, source_id),
            (source_id, source_vdata, source_active, edata, target_id, target_vdata, target_active))
      }, preservePartitioning)
      .filter {
        // Drop any edges that do not have active vertices
        case ((pid, _),
          (source_id, source_vdata, source_active, edata, target_id, target_vdata, target_active)) =>
          source_active || target_active
      }
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

    val numProcs = pidVidPartitioner.numPartitions
    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable =
      edges.map {
        case ((source, target), data) => {
          // val pid = abs((source, target).hashCode()) % numprocs
          val pid = abs(source.hashCode()) % numProcs
          ((pid, source), (target, data))
        }
      }.partitionBy(pidVidPartitioner).cache()

    // The master vertices are used during the apply phase
    var vTable = vertices.map { case (vid, vdata) => (vid, (vdata, true)) }.cache()

    // Create a map from vertex id to the partitions that contain that vertex
    // (vid, pid)
    val vid2pid = eTable.flatMap {
      case ((pid, source), (target, _)) => Array((source, pid), (target, pid))
    }.distinct(1).cache() // what is the role of the number of bins?

    val replicationFactor = vid2pid.count().toDouble / vTable.count().toDouble
    println("Replication Factor: " + replicationFactor)

    // Loop until convergence or there are no active vertices
    var iter = 0
    var nactive = vTable.map {
      case (_, (_, active)) => if (active) 1 else 0
    }.reduce(_ + _);
    //var numActive = vTable.filter(_._2._2).count
    while (iter < niter && nactive > 0) {
      // Begin iteration
      println("\n\n==========================================================")
      println("Begin iteration: " + iter)
      println("Active:          " + nactive)

      iter += 1

      // Gather Phase ---------------------------------------------

      val gatherTable = joinEdgesAndVertices(vTable, eTable, vid2pid)
        .flatMap {
          case ((pid, _),
            (srcId, srcData, srcActive, eData, dstId, dstData, dstActive)) => {
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
      val scatterTable = joinEdgesAndVertices(vTable, eTable, vid2pid)
        .flatMap {
          case ((pid, _),
            (srcId, srcData, srcActive, eData, dstId, dstData, dstActive)) => {
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
        }.reduceByKey(_ || _)

      // update active vertices
      vTable = vTable.leftOuterJoin(scatterTable).map {
        case (vid, ((vdata, _), Some(new_active))) => (vid, (vdata, new_active))
        case (vid, ((vdata, _), None)) => (vid, (vdata, false))
      }.cache()

      // Compute the number active
      nactive = vTable.map {
        case (_, (_, active)) => if (active) 1 else 0
      }.reduce(_ + _);

    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    new Graph(vTable.map { case (vid, (vdata, _)) => (vid, vdata) },
      eTable.map { case ((pid, source), (target, edata)) => ((source, target), edata) })
  } // End of iterate 

  /**
   * The join edges and vertices function returns a table which joins
   * the vertex and edge data.
   */
  private def updateVdataCache(
    vTable: spark.RDD[(Vid, (VD, Boolean))], // vid, data, active
    vid2pid: spark.RDD[(Vid, Pid)], // vid, pid
    vdataCache: spark.broadcast.Broadcast[collection.mutable.HashMap[Vid, (VD, Status)]]) {
    // no return (mutates vdataCache illegally)
    val newInsertions = vTable.join(vid2pid).map {
      case (vid, ((vdata, active), pid)) => ((pid, vid), (vdata, active))
    }.partitionBy(pidVidPartitioner).mapPartitions(iter => iter.map {
      case ((pid, vid), (vdata, active)) => {
        vdataCache.value.put(vid, (vdata, active)) match {
          case Some(_) => 0
          case None => 1
        }
      }
    }, true).reduce(_ + _)
    println("New Map insertsions: " + newInsertions)
  } // end of join edges and vertices

  /**
   * Execute the synchronous powergraph abstraction.
   *
   * gather: (center_vid, edge) => accumulator
   * Todo: Finish commenting
   */
  def iterateUnsafe[A: Manifest](
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

    val numProcs = pidVidPartitioner.numPartitions
    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable =
      edges.map {
        case ((source, target), data) => {
          val pid = abs(source) % numProcs
          ((pid, source), (target, data))
        }
      }.partitionBy(pidVidPartitioner).cache()

    // The master vertices are used during the apply phase
    var vTable = vertices.map { case (vid, vdata) => (vid, (vdata, true)) }.cache()

    // Create a map from vertex id to the partitions that contain that vertex
    // (vid, pid)
    val vid2pid = eTable.flatMap {
      case ((pid, source), (target, _)) => Array((source, pid), (target, pid))
    }.distinct(1).cache() // what is the role of the number of bins?

    // Place a vid2vdat map
    val vdataCache = vertices.context.broadcast(new collection.mutable.HashMap[Vid, (VD, Status)]())
    updateVdataCache(vTable, vid2pid, vdataCache)

    val replicationFactor = vid2pid.count().toDouble / vTable.count().toDouble
    println("Replication Factor: " + replicationFactor)

    // Loop until convergence or there are no active vertices
    var iter = 0
    var nactive = vTable.map {
      case (_, (_, active)) => if (active) 1 else 0
    }.reduce(_ + _);
    //var numActive = vTable.filter(_._2._2).count
    while (iter < niter && nactive > 0) {
      // Begin iteration
      println("\n\n==========================================================")
      println("Begin iteration: " + iter)
      println("Active:          " + nactive)

      iter += 1

      // Gather Phase ---------------------------------------------

      val gatherTable = eTable
        .flatMap {
          case ((pid, srcId), (dstId, eData)) => {
            val (srcData, srcActive) = vdataCache.value(srcId)
            val (dstData, dstActive) = vdataCache.value(dstId)
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

      updateVdataCache(vTable, vid2pid, vdataCache)

      // Scatter Phase ---------------------------------------------
      val scatterTable = eTable
        .flatMap {
          case ((pid, srcId), (dstId, eData)) => {
            val (srcData, srcActive) = vdataCache.value(srcId)
            val (dstData, dstActive) = vdataCache.value(dstId)
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
        }.reduceByKey(_ || _)

      // update active vertices
      vTable = vTable.leftOuterJoin(scatterTable).map {
        case (vid, ((vdata, _), Some(new_active))) => (vid, (vdata, new_active))
        case (vid, ((vdata, _), None)) => (vid, (vdata, false))
      }.cache()

      updateVdataCache(vTable, vid2pid, vdataCache)

      
      // Compute the number active
      nactive = vTable.map {
        case (_, (_, active)) => if (active) 1 else 0
      }.reduce(_ + _);

    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    new Graph(vTable.map { case (vid, (vdata, _)) => (vid, vdata) },
      eTable.map { case ((pid, source), (target, edata)) => ((source, target), edata) })
  } // End of iterateUnsafe

  //
  //  /**
  //   * Execute the synchronous powergraph abstraction.
  //   *
  //   * gather: (center_vid, edge) => (new edge data, accumulator)
  //   * Todo: Finish commenting
  //   */
  //  def iterateWithEdgeModification[A: Manifest](
  //    gather: (Vid, Edge[VD, ED]) => (ED, A),
  //    merge: (A, A) => A,
  //    default: A,
  //    apply: (Vertex[VD], A) => VD,
  //    scatter: (Vid, Edge[VD, ED]) => (ED, Boolean),
  //    niter: Int,
  //    gatherEdges: EdgeDirection.Value = EdgeDirection.In,
  //    scatterEdges: EdgeDirection.Value = EdgeDirection.Out) = {
  //
  //    ClosureCleaner.clean(gather)
  //    ClosureCleaner.clean(merge)
  //    ClosureCleaner.clean(apply)
  //    ClosureCleaner.clean(scatter)
  //
  //    val numprocs = pidVidPartitioner.numPartitions
  //    // Partition the edges over machines.  The part_edges table has the format
  //    // ((pid, source), (target, data))
  //    var eTable =
  //      edges.map {
  //        case ((source, target), data) => {
  //          // val pid = abs((source, target).hashCode()) % numprocs
  //          val pid = abs(source.hashCode()) % numprocs
  //          ((pid, source), (target, data))
  //        }
  //      }.partitionBy(pidVidPartitioner).persist()
  //
  //    // The master vertices are used during the apply phase
  //    var vTable = vertices.map { case (vid, vdata) => (vid, (vdata, true)) }.persist()
  //
  //    // Create a map from vertex id to the partitions that contain that vertex
  //    // (vid, pid)
  //    val vid2pid = eTable.flatMap {
  //      case ((pid, source), (target, _)) => Array((source, pid), (target, pid))
  //    }.distinct(1).persist() // what is the role of the number of bins?
  //
  //    val replicationFactor = vid2pid.count().toDouble / vTable.count().toDouble
  //    println("Replication Factor: " + replicationFactor)
  //
  //    // Loop until convergence or there are no active vertices
  //    var iter = 0
  //    var nactive = vTable.map {
  //      case (_, (_, active)) => if (active) 1 else 0
  //    }.reduce(_ + _);
  //    //var numActive = vTable.filter(_._2._2).count
  //    while (iter < niter && nactive > 0) {
  //      // Begin iteration
  //      println("\n\n==========================================================")
  //      println("Begin iteration: " + iter)
  //      println("Active:          " + nactive)
  //
  //      iter += 1
  //
  //      // Gather Phase ---------------------------------------------
  //
  //      // Compute the accumulator for each vertex
  //      val join1 = joinEdgesAndVertices(vTable, eTable, vid2pid).persist()
  //      println(join1.count)
  //
  //      val gTable = join1.mapValues {
  //        case (sourceId, sourceVdata, sourceActive, edata,
  //          targetId, targetVdata, targetActive) => {
  //          val sourceVertex = new Vertex[VD](sourceId, sourceVdata)
  //          val targetVertex = new Vertex[VD](targetId, targetVdata)
  //          var newEdata = edata
  //          var targetAccum: Option[A] = None
  //          var sourceAccum: Option[A] = None
  //          if (targetActive && (gatherEdges == EdgeDirection.In ||
  //            gatherEdges == EdgeDirection.Both)) { // gather on the target
  //            val edge = new Edge(sourceVertex, targetVertex, newEdata);
  //            val (edataRet, accRet) = gather(targetId, edge)
  //            targetAccum = Option(accRet)
  //            newEdata = edataRet
  //          }
  //          if (sourceActive && (gatherEdges == EdgeDirection.Out ||
  //            gatherEdges == EdgeDirection.Both)) { // gather on the source
  //            val edge = new Edge(sourceVertex, targetVertex, newEdata);
  //            val (edataRet, accRet) = gather(sourceId, edge)
  //            sourceAccum = Option(accRet);
  //            newEdata = edataRet;
  //          }
  //          (sourceId, sourceAccum, targetId, targetAccum, newEdata)
  //        }
  //      }.cache()
  //
  //      println(gTable.count())
  //
  //      // update the edge data
  //      eTable = gTable.mapValues {
  //        case (sourceId, _, targetId, _, edata) => (targetId, edata)
  //      }
  //
  //      // mapPartitions { iter =>
  //      //   val reuseRow = new Array[Object](5)
  //      //   iter.map {
  //      //     reusedRow(0) = ...
  //      //     (1) = ...
  //      //     reusedRow
  //      //   }
  //      // }
  //
  //      // Compute the final sum
  //      val accumTable: RDD[(Vid, A)] = gTable.flatMap {
  //        case (_, (sourceId, Some(sourceAccum), targetId, Some(targetAccum), _)) =>
  //          Array((sourceId, sourceAccum), (targetId, targetAccum))
  //        case (_, (sourceId, None, targetId, Some(targetAccum), _)) =>
  //          Array((targetId, targetAccum))
  //        case (_, (sourceId, Some(sourceAccum), targetId, None, _)) =>
  //          Array((sourceId, sourceAccum))
  //        case (_, (sourceId, None, targetId, None, _)) =>
  //          Iterator.empty
  //      }.reduceByKey(merge)
  //
  //      // Apply Phase ---------------------------------------------
  //      // Merge with the gather result
  //      vTable = vTable.leftOuterJoin(accumTable).mapPartitions(
  //        iter => iter.map { // Execute the apply if necessary
  //          case (vid, ((data, true), Some(accum))) =>
  //            (vid, (apply(new Vertex[VD](vid, data), accum), true))
  //          case (vid, ((data, true), None)) =>
  //            (vid, (apply(new Vertex[VD](vid, data), default), true))
  //          case (vid, ((data, false), _)) => (vid, (data, false))
  //        }).cache()
  //      println(vTable.count())
  //
  //      // Scatter Phase ---------------------------------------------
  //      val sTable = joinEdgesAndVertices(vTable, eTable, vid2pid).mapValues {
  //        case (sourceId, sourceVdata, sourceActive, edata,
  //          targetId, targetVdata, targetActive) => {
  //          val sourceVertex = new Vertex[VD](sourceId, sourceVdata)
  //          val targetVertex = new Vertex[VD](targetId, targetVdata)
  //          var newEdata = edata
  //          var newTargetActive = false
  //          var newSourceActive = false
  //          if (targetActive && (scatterEdges == EdgeDirection.In ||
  //            scatterEdges == EdgeDirection.Both)) { // scatter on the target
  //            val edge = new Edge(sourceVertex, targetVertex, newEdata);
  //            val result = scatter(targetId, edge)
  //            newSourceActive = result._2
  //            newEdata = result._1
  //          }
  //          if (sourceActive && (scatterEdges == EdgeDirection.Out ||
  //            scatterEdges == EdgeDirection.Both)) { // scatter on the source
  //            val edge = new Edge(sourceVertex, targetVertex, newEdata);
  //            val result = scatter(sourceId, edge)
  //            newTargetActive = result._2;
  //            newEdata = result._1;
  //          }
  //          (sourceId, newSourceActive, targetId, newTargetActive, newEdata)
  //        }
  //      }.cache()
  //
  //      // update the edge data
  //      eTable = sTable.mapValues {
  //        case (sourceId, _, targetId, _, edata) => (targetId, edata)
  //      }
  //
  //      val activeVertices = sTable.flatMap {
  //        case (_, (sourceId, sourceActive, targetId, targetActive, _)) =>
  //          Array((sourceId, sourceActive), (targetId, targetActive))
  //      }.reduceByKey(_ || _)
  //
  //      // update active vertices
  //      vTable = vTable.leftOuterJoin(activeVertices).map {
  //        case (vid, ((vdata, _), Some(new_active))) => (vid, (vdata, new_active))
  //        case (vid, ((vdata, _), None)) => (vid, (vdata, false))
  //      }.cache()
  //
  //      // Compute the number active
  //      nactive = vTable.map {
  //        case (_, (_, active)) => if (active) 1 else 0
  //      }.reduce(_ + _);
  //
  //    }
  //    println("=========================================")
  //    println("Finished in " + iter + " iterations.")
  //
  //    // Collapse vreplicas, edges and retuen a new graph
  //    new Graph(vTable.map { case (vid, (vdata, _)) => (vid, vdata) },
  //      eTable.map { case ((pid, source), (target, edata)) => ((source, target), edata) })
  //  } // End of iterate gas

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
        ((source.trim.toInt, target.trim.toInt), edata)
      }).cache()

    // Parse the vertex data table
    val vertices = edges.flatMap {
      case ((source, target), _) => List((source, 1), (target, 1))
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
        ((src.trim.toInt, dst.trim.toInt), edata)
      }).toArray
    val edges = sc.parallelize(content).cache()
    println("determining vertices")
    // Parse the vertex data table
    val vertices = edges.flatMap {
      case ((source, target), _) => List((source, 1), (target, 1))
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
    val ball = Array((1 -> 2, 0), (2 -> 3, 0), (3 -> 1, 0))
    val chain = Array((2 -> 4, 0), (4 -> 5, 0), (5 -> 6, 0))
    val edges = ball ++ chain
    val vertices = edges.flatMap {
      case ((source, target), value) => Array(source, target)
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
      ((source, target), i * numCycles + j)
    }
    // Construct the vertices enumerated with the cycle number
    val vertices = edges.flatMap {
      case ((source, target), value) => Array(source, target)
    }.distinct.map(vid => (vid, (vid / numCycles) * numCycles))
    new Graph(sc.parallelize(vertices), sc.parallelize(edges))
  }

} // End of Graph Object

// class MyRegistrator extends KryoRegistrator {
//   override def registerClasses(kryo: Kryo) {
//     kryo.register(classOf[MyClass1])
//     kryo.register(classOf[MyClass2])
//   }
// }

///**
// * Test object for graph class
// */
//object GraphTest {
//
//
//
//
//
//
//
//  def main(args: Array[String]) {
//
//    // System.setProperty("spark.serializer", "spark.KryoSerializer")
//    // System.setProperty("spark.kryo.registrator", classOf[KryoRegistrator].getName)
//
//    val filename = if (args.length > 0) args(0) else ""
//    val spark_master = if (args.length > 1) args(1) else "local[4]"
//    println("Spark Master: " + spark_master)
//    println("Graph file:   " + filename)
//
//    val jobname = "graphtest"
//    //  val spark_home = "/home/jegonzal/local/spark"
//    //  val graphlab_jar = List("/home/jegonzal/Documents/scala_graphlab/graphlab/target/scala-2.9.2/graphlab_2.9.2-1.0-spark.jar")
//    //    val hdfs_path = "hdfs://128.2.204.196/users/jegonzal/google.tsv"
//    var sc: SparkContext = null;
//    try {
//      sc = new SparkContext(spark_master, jobname) // , spark_home, graphlab_jar)
//      val graph = if (!filename.isEmpty) load_file(sc, filename) else toy_graph(sc)
//      test_connected_component(graph)
//    } catch {
//      case e: Throwable => println(e)
//    }
//
//    if (sc != null) sc.stop()
//
//  }
//} // end of GraphTest
//
//


