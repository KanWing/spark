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


class Timer {
  var lastTime = System.currentTimeMillis
  def tic = {
    val currentTime = System.currentTimeMillis
    val elapsedTime = (currentTime - lastTime)/1000.0
    lastTime = currentTime
    elapsedTime
  }
}


/** Things to try
 * extend KyroSerializable
 *
 */

/**
 * Class containing the id and value of a vertex
 */
class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD ](
  var id: Vid, var data: VD, var isActive: Status = true)

/**
 * Class containing both vertices and the edge data associated with an edge
 */
class Edge[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD,
@specialized(Char, Int, Boolean, Byte, Long, Float, Double)ED](
  var source: Vertex[VD], var target: Vertex[VD], var data: ED) {
  def otherVertex(vid: Vid) = { if (source.id == vid) target else source; }
  def vertex(vid: Vid) = { if (source.id == vid) source else target; }
} // end of class edge


// class EdgeRecord[
// @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED : ClassManifest](
//   val sourceId: Vid, val targetId: Vid, val data: ED)


class EdgeBlockRecord[
@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED](
  val sourceId: IntArrayList = new IntArrayList, val targetId: IntArrayList = new IntArrayList,
  val data: ArrayBuffer[ED] = new ArrayBuffer[ED]) {
  def add(srcId: Vid, dstId: Vid, eData: ED) {
    sourceId.add(srcId)
    targetId.add(dstId)
    data.add(eData)
  }
}


// class VertexReplica[
// @specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD : ClassManifest](
//   var id: Vid, var data: VD, var isActive: Status) extends KryoSerializable {
//   def read(kryo: Kryo, in: io.Input) {
//     id = in.readInt()
//     data = kryo.readObject(in, data.getClass)
//     isActive = in.readBoolean()
//   }
//   def write(kryo: Kryo, out: io.Output) {
//     out.writeInt(id)
//     kryo.writeObject(out, data)
//     out.writeBoolean(isActive)
//   }
// }


// class VertexRecord[
// @specialized(Char, Int, Boolean, Byte, Long, Float, Double)VD : ClassManifest](
//   var data: VD, var isActive: Status, var pids: Array[Pid]) {
//   def replicate(id: Vid) = pids.iterator.map(pid => (pid, new VertexReplica(id, data, isActive)))
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
  val vertices: spark.RDD[(Vid, VD)],
  val edges: spark.RDD[(Vid, Vid, ED)]) {

  /**
   * The number of unique vertices in this graph
   */
  lazy val numVertices = vertices.count().toInt

  /**
   * The number of unique edges in this graph
   */
  lazy val numEdges = edges.count().toInt

  var numEdgePart = 4
  var numVertexPart = 4

  /**
   * Create a cached in memory copy of the graph.
   */
  def cache(): Graph[VD, ED] = {
    new Graph(vertices.cache(), edges.cache())
  }


  //

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
    scatter: (Vid, Edge[VD, ED]) => Status,
    niter: Int,
    gatherEdges: EdgeDirection.Value = EdgeDirection.In,
    scatterEdges: EdgeDirection.Value = EdgeDirection.Out) = {

    ClosureCleaner.clean(gather)
    ClosureCleaner.clean(merge)
    ClosureCleaner.clean(apply)
    ClosureCleaner.clean(scatter)

    val numEdgePartLocal = numEdgePart
    val numVertexPartLocal = numVertexPart

    val sc = edges.context
    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))

    var eTable = edges
      .map { case (source, target, data) =>
        (math.abs(source) % numEdgePartLocal, (source, target, data))
      }
      .partitionBy(new HashPartitioner(numEdgePartLocal))
      .mapPartitionsWithSplit({ (pid, iter) =>
        val edgeBlock = new EdgeBlockRecord[ED]()
        iter.foreach{ case (_, (srcId, dstId, data)) => edgeBlock.add(srcId, dstId, data) }
        Iterator((pid, edgeBlock))
      }, preservesPartitioning = true).cache()

    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numVertexPartLocal);


    val vid2pid : RDD[(Int, Seq[Pid])] = eTable.flatMap {
        case (pid, ebr) => {
          val sourceIdArray = ebr.sourceId
          val targetIdArray = ebr.targetId
          val vmap = new it.unimi.dsi.fastutil.ints.IntOpenHashSet //(1000000)
          var i = 0
          while(i < sourceIdArray.length) {
            vmap.add(sourceIdArray(i))
            vmap.add(targetIdArray(i))
            i += 1
          }
          val pidLocal = pid
          vmap.iterator. map( vid => (vid.intValue, pidLocal) )
        }
      }.groupByKey(vTablePartitioner)


    var vTable = vertices.partitionBy(vTablePartitioner).leftOuterJoin(vid2pid).mapValues {
        case (vdata, None) => (vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => (vdata, true, pids.toArray)
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
        (edge: Edge[VD, ED], srcAcc: VMapRecord[VD,A], dstAcc: VMapRecord[VD,A]) => {
          if (edge.target.isActive && (gatherEdges == EdgeDirection.In ||
            gatherEdges == EdgeDirection.Both)) { // gather on the target
            dstAcc.add(gather(edge.target.id, edge), merge)
          }
          if (edge.source.isActive && (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
            srcAcc.add(gather(edge.source.id, edge), merge)
          }
        }, default)
        .combineByKey((i: A) => i, merge, null, vTablePartitioner, false)

      // Apply Phase ===========================================================
      // Merge with the gather result
      vTable = vTable.leftOuterJoin(gatherTable).mapPartitions( _.map {
        case (vid, ((vdata, isActive, pids), accum)) => (vid,
            ((if(isActive) apply(new Vertex(vid, vdata), accum.getOrElse(default)) else vdata),
             isActive, pids))
        }, preservesPartitioning = true).cache()


      // Scatter Phase =========================================================
      val scatterTable = new GraphShardRDD(vTable, eTable,
        (edge: Edge[VD,ED], srcAcc: VMapRecord[VD,Status], dstAcc: VMapRecord[VD,Status]) => {
          if (edge.target.isActive && (scatterEdges == EdgeDirection.In ||
            scatterEdges == EdgeDirection.Both)) { // gather on the target
            srcAcc.add(scatter(edge.target.id, edge), _ || _)
          }
          if (edge.source.isActive && (scatterEdges == EdgeDirection.Out ||
          scatterEdges == EdgeDirection.Both)) { // gather on the source
            dstAcc.add(scatter(edge.source.id, edge), _ || _)
          }
        }, false)
        .combineByKey((i: Status) => i, (_: Status) || (_:Status),
          null, vTablePartitioner, false)


      // update active vertices
      numActive.value = 0

      vTable = vTable.leftOuterJoin(scatterTable).mapPartitions( _.map {
        case (vid, ((vdata, isActive, pids), isActiveOption)) => {
          val newIsActive = isActiveOption.getOrElse(false)
          if(newIsActive) numActive +=1
          (vid, (vdata, newIsActive, pids))
          }
        }, preservesPartitioning = true).cache()

      // Force the vtable to be computed and determine the number of active vertices
      vTable.foreach(i => ())
      println("Number of active vertices: " + numActive.value)

      iter += 1
    }
    println("=========================================")
    println("Finished in " + iter + " iterations.")

    // Collapse vreplicas, edges and retuen a new graph
    // new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
    //   eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
    new Graph(vTable.map { case (vid, (vdata,_,_)) => (vid, vdata) }, edges)

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

    val numEdgePartLocal = numEdgePart
    val numVertexPartLocal = numVertexPart

    val sc = edges.context

    val timer = new Timer

    // Partition the edges over machines.  The part_edges table has the format
    // ((pid, source), (target, data))

     var eTable = edges
      .map { case (source, target, data) =>
        (math.abs(source) % numEdgePartLocal, (source, target, data))
      }
      .partitionBy(new HashPartitioner(numEdgePartLocal))
      .mapPartitionsWithSplit({ (pid, iter) =>
        val edgeBlock = new EdgeBlockRecord[ED]()
        iter.foreach{ case (_, (srcId, dstId, data)) => edgeBlock.add(srcId, dstId, data) }
        Iterator((pid, edgeBlock))
      }, preservesPartitioning = true).cache()

    println("EdgeTable count: " + eTable.count)
    println("Time: " + timer.tic)

    // The master vertices are used during the apply phase
    val vTablePartitioner = new HashPartitioner(numVertexPartLocal)


    val vid2pid : RDD[(Int, Seq[Pid])] = eTable.flatMap {
        case (pid, ebr) => {
          val sourceIdArray = ebr.sourceId
          val targetIdArray = ebr.targetId
          val vSet = new it.unimi.dsi.fastutil.ints.IntOpenHashSet
          var i = 0
          while(i < sourceIdArray.length) {
            vSet.add(sourceIdArray(i))
            vSet.add(targetIdArray(i))
            i += 1
          }
          val pidLocal = pid
          vSet.iterator.map( vid => (vid.intValue, pidLocal) )
        }
      }.groupByKey(vTablePartitioner)

 //   println("Vid2Pid Count: " + vid2pid.count)
 //   println("Time: " + timer.tic)


    var vTable = vertices.partitionBy(vTablePartitioner).leftOuterJoin(vid2pid).mapValues {
        case (vdata, None) => (vdata, true, Array.empty[Pid])
        case (vdata, Some(pids)) => (vdata, true, pids.toArray)
      }.cache()

    println("VTable Count: " + vTable.count)
    println("Time: " + timer.tic)

    // Loop until convergence or there are no active vertices
    var iter = 0
    while (iter < numIter)  {

      // println("Starting Iteration:")

      // Gather Phase =========================================================
      val gatherTable = new GraphShardRDD(vTable, eTable,
        (edge: Edge[VD, ED], srcAcc: VMapRecord[VD,A], dstAcc: VMapRecord[VD,A]) => {
          if (/*edge.target.isActive && */(gatherEdges == EdgeDirection.In ||
            gatherEdges == EdgeDirection.Both)) { // gather on the target
            dstAcc.add(gather(edge.target.id, edge), merge)
          }
          if (/*edge.source.isActive &&*/ (gatherEdges == EdgeDirection.Out ||
          gatherEdges == EdgeDirection.Both)) { // gather on the source
            srcAcc.add(gather(edge.source.id, edge), merge)
          }
        }, default)
        .combineByKey((i: A) => i, merge, null, vTablePartitioner, false)

       // println("Gather Table Count: " + gatherTable.count)
       // println("Time: " + timer.tic)

      // Apply Phase ===========================================================
      // Merge with the gather result

      vTable = vTable.leftOuterJoin(gatherTable).mapPartitions( _.map {
        case (vid, ((vdata, isActive, pids), accum)) => (vid,
          ((if(isActive) apply(new Vertex(vid, vdata), accum.getOrElse(default)) else vdata),
          isActive, pids))
        }, preservesPartitioning = true).cache()

      // vTable.take(10).foreach(println(_))
      // println("VTable Count: " + vTable.count)
      // vTable.foreach(i => ())
      // println("Time: " + timer.tic)
      // end of iteration
      iter += 1
    }

    vTable.foreach(i => ())
    println("Finished " + numIter + " iterations in " + timer.tic + " seconds.")


    // Collapse vreplicas, edges and retuen a new graph
    // new Graph(vTable.map { case (vid, VertexRecord(vdata, _, _)) => (vid, vdata) },
    //   eTable.map { case (pid, EdgeRecord(source, target, edata)) => (source, target, edata) })
    new Graph(vTable.map { case (vid, (vdata,_,_)) => (vid, vdata) }, edges)

  } // End of iterate





} // End of Graph RDD

/**
 * Graph companion object
 */
object Graph {




  def kryoRegister[VD: Manifest, ED: Manifest](kryo: Kryo) {
    //kryo.register(classManifest[VD].erasure)
    // kryo.register(classManifest[ED].erasure)
    kryo.register(classOf[(Vid,Vid,ED)])
    kryo.register(classOf[(Vid,ED)])
    kryo.register(classOf[EdgeBlockRecord[ED]])

    // kryo.register(classOf[(Vid,Vid,Float)])
    // kryo.register(classOf[(Vid,Float)])
    // kryo.register(classOf[VertexRecord[(Int,Float)]])
    // kryo.register(classOf[VertexReplica[(Int,Float)]])
    // kryo.register(classOf[(Vid, VertexRecord[(Int,Float)])])
    // kryo.register(classOf[(Pid, VertexReplica[(Int,Float)])])
    // kryo.register(classOf[EdgeRecord[Float]])
    // kryo.register(classOf[EdgeBlockRecord[Float]])

  }

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: Manifest](sc: SparkContext,
    fname: String, edgeParser: Array[String] => ED) = {

    // Parse the edge data table
    val edges = sc.textFile(fname).map(
      line => {
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0)
        val target = lineArray(1)
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        (source.trim.toInt, target.trim.toInt, edata)
      })

    // Parse the vertex data table
    val vertices = edges.flatMap {
      case (source, target, _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _)

    val graph = new Graph[Int, ED](vertices, edges)

    println("Loaded graph:" +
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

  /**
   * Make a regular grid graph
   **/
  def grid(sc: SparkContext, numRows: Int = 5, numCols: Int = 5) = {
    def coord(vid: Int) = (vid % numRows, vid / numRows)
    val vertices = sc.parallelize( 0 until (numRows * numCols) ).map(vid => (vid, coord(vid)))
    def index(r: Int, c:Int) = (r + c * numRows)
    val edges = vertices.flatMap{ case (vid, (r,c)) =>
      (if(r+1 < numRows) List((vid, index(r+1,c), 1.0F)) else List.empty) ++
        (if(c+1 < numCols) List((vid, index(r,c+1), 1.0F)) else List.empty)
    }
    new Graph(vertices, edges)
 }

} // End of Graph Object


