package spark.graphlab

import spark.SparkContext
import spark.SparkContext._
import spark.HashPartitioner
import spark.storage.StorageLevel
import spark.KryoRegistrator
import spark.ClosureCleaner
import spark.RDD

import it.unimi.dsi.fastutil.ints.IntArrayList
import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConversions._

import com.esotericsoftware.kryo._



/**
 * Class containing the id and value of a vertex
 */
case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD](
  val id: Vid, val data: VD, val isActive: Status = true)

/**
 * Class containing both vertices and the edge data associated with an edge
 */
case class Edge[VD, @specialized(Char, Int, Boolean, Byte, Long, Float, Double)ED](
  val source: Vertex[VD], val target: Vertex[VD], val data: ED) {
  def otherVertex(vid: Vid) = { if (source.id == vid) target else source; }
  def vertex(vid: Vid) = { if (source.id == vid) source else target; }
} // end of class edge


case class EdgeRecord[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED](
  val sourceId: Vid, val targetId: Vid, val data: ED)


case class EdgeBlockRecord[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED](
  val sourceId: IntArrayList = new IntArrayList, val targetId: IntArrayList = new IntArrayList,
  val data: ArrayBuffer[ED] = new ArrayBuffer[ED]) {
  def add(rec: EdgeRecord[ED]) { add(rec.sourceId, rec.targetId, rec.data) }
  def add(srcId: Vid, dstId: Vid, eData: ED) {
    sourceId.add(srcId)
    targetId.add(dstId)
    data.add(eData)
  }
}


case class VertexReplica[@specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD](
  val id: Vid, val data: VD, val isActive: Status)

case class VertexRecord[@specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD](
  val data: VD, val isActive: Status, val pids: Array[Pid]) {
  def replicate(id: Vid) = pids.iterator.map(pid => (pid, VertexReplica(id, data, isActive)))
}




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

   var eTable = edges
      .map { case (source, target, data) =>
        (math.abs(source) % numProcs, EdgeRecord(source, target, data))
      }
      .partitionBy(new HashPartitioner(numProcs))
      .mapPartitionsWithSplit({ (pid, iter) =>
        val edgeBlock = EdgeBlockRecord[ED]()
        iter.foreach{ case (_, record) => edgeBlock.add(record) }
        Iterator((pid, edgeBlock))
      }, preservesPartitioning = true).cache()

    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numProcs)


    val vid2pid : RDD[(Int, Seq[Int])] = eTable.flatMap {
        case (pid, EdgeBlockRecord(sourceIdArray, targetIdArray, _)) => {
          val vmap = new it.unimi.dsi.fastutil.ints.IntOpenHashSet(1000000)
          var i = 0
          while(i < sourceIdArray.length) {
            vmap.add(sourceIdArray(i))
            vmap.add(targetIdArray(i))
            i += 1
          }
          val pidLocal = pid
          // new Iterator[(Int,Int)] {
          //   val iter = vmap.iterator
          //   def hasNext = iter.hasNext
          //   def next() = (pid, iter.nextInt())
          // }
          vmap.iterator. map( vid => (vid.intValue, pidLocal) )
        }
      }.groupByKey(vTablePartitioner)




    var vTable = vertices.partitionBy(vTablePartitioner).leftOuterJoin(vid2pid).mapValues {
        case (vdata, None) => VertexRecord(vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => VertexRecord(vdata, true, pids.toArray)
      }.cache()


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
      val gatherTable = new GraphShardRDD(vTable, eTable,
        { edge: Edge[VD, ED] =>
        (if (edge.target.isActive && (gatherEdges == EdgeDirection.In ||
          gatherEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.target.id, gather(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.isActive && (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.source.id, gather(edge.source.id, edge)))
        } else List.empty)
      }, merge, default)
      .combineByKey((i: A) => i, merge, null, vTablePartitioner, false)

      println("Gather table size: " + gatherTable.count)

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
      val scatterTable = new GraphShardRDD(vTable, eTable,
        { edge: Edge[VD, ED] =>
        //var accum = List.empty[(Vid, Boolean)]
        (if (edge.target.isActive && (scatterEdges == EdgeDirection.In ||
          scatterEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.source.id, scatter(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.isActive && (scatterEdges == EdgeDirection.Out ||
          scatterEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.target.id, scatter(edge.source.id, edge)))
        } else List.empty)
      }, (_: Boolean) || (_:Boolean), false)
      .combineByKey((i: Boolean) => i, (_: Boolean) || (_:Boolean),
        null, vTablePartitioner, false)

      println("Scatter table size: " + scatterTable.count)

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

      // Force the vtable to be computed and determine the number of active vertices
      vTable.foreach(i => ())
      println("Active vtable: " + vTable.filter(_._2.isActive).count)

      println("Number of active vertices: " + numActive.value)

      iter += 1
    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    // new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
    //   eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
    new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) }, edges)

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
    val numProcs = 10

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))
    var eTable = edges
      .map { case (source, target, data) =>
        (math.abs(source) % numProcs, EdgeRecord(source, target, data))
      }
      .partitionBy(new HashPartitioner(numProcs))
      .mapPartitionsWithSplit({ (pid, iter) =>
        val edgeBlock = EdgeBlockRecord[ED]()
        iter.foreach{ case (_, record) => edgeBlock.add(record) }
        Iterator((pid, edgeBlock))
      }, preservesPartitioning = true).cache()


    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numProcs)

    val vid2pid : RDD[(Int, Seq[Int])] = eTable.flatMap {
        case (pid, EdgeBlockRecord(sourceIdArray, targetIdArray, _)) => {
          val vmap = new it.unimi.dsi.fastutil.ints.IntOpenHashSet(1000000)
          var i = 0
          while(i < sourceIdArray.length) {
            vmap.add(sourceIdArray(i))
            vmap.add(targetIdArray(i))
            i += 1
          }
          val pidLocal = pid
          // new Iterator[(Int,Int)] {
          //   val iter = vmap.iterator
          //   def hasNext = iter.hasNext
          //   def next() = (pid, iter.nextInt())
          // }
          vmap.iterator. map( vid => (vid.intValue, pidLocal) )
        }
      }.groupByKey(vTablePartitioner)




    var vTable = vertices.partitionBy(vTablePartitioner).leftOuterJoin(vid2pid).mapValues {
        case (vdata, None) => VertexRecord(vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => VertexRecord(vdata, true, pids.toArray)
      }.cache()


    // Loop until convergence or there are no active vertices
    var iter = 0
    while (iter < numIter)  {

      // Gather Phase =========================================================
      val gatherTable = new GraphShardRDD(vTable, eTable,
        { edge: Edge[VD, ED] =>
        (if (edge.target.isActive && (gatherEdges == EdgeDirection.In ||
          gatherEdges == EdgeDirection.Both)) { // gather on the target
          List((edge.target.id, gather(edge.target.id, edge)))
        } else List.empty) ++
        (if (edge.source.isActive && (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
          List((edge.source.id, gather(edge.source.id, edge)))
        } else List.empty)
      }, merge, default).combineByKey((i: A) => i, merge, null, vTablePartitioner, false)

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

      // end of iteration
      iter += 1
    }

    // Collapse vreplicas, edges and retuen a new graph
    // new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
    //   eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
    new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) }, edges)

  } // End of iterate





} // End of Graph RDD

/**
 * Graph companion object
 */
object Graph {



  def kryoRegister[VD,ED](kryo: Kryo) {
    kryo.register(classOf[(Vid,Vid,ED)])
    kryo.register(classOf[(Vid,ED)])
    kryo.register(classOf[VertexRecord[VD]])
    kryo.register(classOf[VertexReplica[VD]])
    kryo.register(classOf[EdgeRecord[ED]])
    kryo.register(classOf[EdgeBlockRecord[ED]])
  }

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


