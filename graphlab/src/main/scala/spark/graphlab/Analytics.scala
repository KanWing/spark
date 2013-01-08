package spark.graphlab

import spark._
import spark.SparkContext._
import com.esotericsoftware.kryo._


class AnalyticsKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[(Int,Float,Float)])
    Graph.kryoRegister[(Int,Float,Float), Float](kryo)
    Graph.kryoRegister[(Int,Float), Float](kryo)
    Graph.kryoRegister[Int, Float](kryo)
    Graph.kryoRegister[Float, Float](kryo)
  }
}


object Analytics {

  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def dynamicPageRank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], tol: Float, maxIter: Int = 10) = {
    graph.edges.cache
    // Compute the out degree of each vertex
    val outDegree = graph.edges.map { case (src, target, data) => (src, 1)}.reduceByKey(_ + _)
    val vertices = graph.vertices.leftOuterJoin(outDegree).mapValues {
      case (_, Some(deg)) => (deg, 1.0F, 1.0F)
      case (_, None) => (0, 1.0F, 1.0F)
    }.cache

    val edges = graph.edges
    val pageRankGraph = new Graph(vertices, edges)
    // Run PageRank
    pageRankGraph.iterateDynamic(
      (me_id, edge) => edge.source.data._2 / edge.source.data._1, // gather
      (a: Float, b: Float) => a + b, // merge
      0F,
      (vertex, a: Float) => (vertex.data._1, (0.15F + 0.85F * a), vertex.data._2), // apply
      (me_id, edge) => math.abs(edge.source.data._2 - edge.source.data._1) > tol, // scatter
      maxIter).vertices.mapValues { case (degree, rank, oldRank) => rank }
    //    println("Computed graph: #edges: " + graph_ret.numEdges + "  #vertices" + graph_ret.numVertices)
    //    graph_ret.vertices.take(10).foreach(println)
  }



  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def pageRank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], maxIter: Int) = {
    graph.edges.cache
    // Compute the out degree of each vertex
    val outDegree = graph.edges.map { case (src, target, data) => (src, 1)}.reduceByKey(_ + _)
    val vertices = graph.vertices.leftOuterJoin(outDegree).mapValues {
      case (_, Some(deg)) => (deg, 1.0F)
      case (_, None) => (0, 1.0F)
    }.cache
    val edges = graph.edges //.map(v => None)
    val pageRankGraph = new Graph(vertices, edges)
    // Run PageRank
    pageRankGraph.iterateStatic(
      (me_id, edge) => edge.source.data._2 / edge.source.data._1, // gather
      (a: Float, b: Float) => a + b, // merge
      0F,
      (vertex, a: Float) => (vertex.data._1, (0.15F + 0.85F * a)), // apply
      maxIter).vertices.mapValues { case (degree, rank) => rank }
    //    println("Computed graph: #edges: " + graph_ret.numEdges + "  #vertices" + graph_ret.numVertices)
    //    graph_ret.vertices.take(10).foreach(println)
  }


  /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def dynamicConnectedComponents[VD: Manifest, ED: Manifest](graph: Graph[VD, ED],
    numIter: Int = Int.MaxValue) = {

    val vertices = graph.vertices.mapPartitions(iter => iter.map { case (vid, _) => (vid, vid) })
    val edges = graph.edges // .mapValues(v => None)
    val ccGraph = new Graph(vertices, edges)

    ccGraph.iterateDynamic(
      (me_id, edge) => edge.otherVertex(me_id).data, // gather
      (a: Int, b: Int) => math.min(a, b), // merge
      Integer.MAX_VALUE,
      (v, a: Int) => math.min(v.data, a), // apply
      (me_id, edge) => edge.otherVertex(me_id).data > edge.vertex(me_id).data, // scatter
      numIter,
      gatherEdges = EdgeDirection.Both,
      scatterEdges = EdgeDirection.Both).vertices
    //
    //    graph_ret.vertices.collect.foreach(println)
    //    graph_ret.edges.take(10).foreach(println)
  }


  /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def connectedComponents[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], numIter: Int) = {

    val vertices = graph.vertices.mapPartitions(iter => iter.map { case (vid, _) => (vid, vid) })
    val edges = graph.edges // .mapValues(v => None)
    val ccGraph = new Graph(vertices, edges)


    ccGraph.iterateStatic(
      (me_id, edge) => edge.otherVertex(me_id).data, // gather
      (a: Int, b: Int) => math.min(a, b), // merge
      Integer.MAX_VALUE,
      (v, a: Int) => math.min(v.data, a), // apply
      numIter,
      gatherEdges = EdgeDirection.Both).vertices
    //
    //    graph_ret.vertices.`.foreach(println)
    //    graph_ret.edges.take(10).foreach(println)
  }



 /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def dynamicShortestPath[VD: Manifest, ED: Manifest](graph: Graph[VD, Float],
    sources: List[Int], numIter: Int) = {
    val sourceSet = sources.toSet
    val vertices = graph.vertices.mapPartitions(
      iter => iter.map {
        case (vid, _) => (vid, (if(sourceSet.contains(vid)) 0.0F else Float.MaxValue) )
        });

    val edges = graph.edges // .mapValues(v => None)
    val spGraph = new Graph(vertices, edges)

    val niterations = Int.MaxValue
    spGraph.iterateDynamic(
      (me_id, edge) => edge.otherVertex(me_id).data + edge.data, // gather
      (a: Float, b: Float) => math.min(a, b), // merge
      Float.MaxValue,
      (v, a: Float) => math.min(v.data, a), // apply
      (me_id, edge) => edge.vertex(me_id).data + edge.data < edge.otherVertex(me_id).data, // scatter
      numIter,
      gatherEdges = EdgeDirection.In,
      scatterEdges = EdgeDirection.Out).vertices
  }


 /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def shortestPath[VD: Manifest, ED: Manifest](graph: Graph[VD, Float],
    sources: List[Int], numIter: Int) = {
    val sourceSet = sources.toSet
    val vertices = graph.vertices.mapPartitions(
      iter => iter.map {
        case (vid, _) => (vid, (if(sourceSet.contains(vid)) 0.0F else Float.MaxValue) )
        });

    val edges = graph.edges // .mapValues(v => None)
    val spGraph = new Graph(vertices, edges)

    val niterations = Int.MaxValue
    spGraph.iterateStatic(
      (me_id, edge) => edge.otherVertex(me_id).data + edge.data, // gather
      (a: Float, b: Float) => math.min(a, b), // merge
      Float.MaxValue,
      (v, a: Float) => math.min(v.data, a), // apply
      numIter,
      gatherEdges = EdgeDirection.In).vertices
  }



  def main(args: Array[String]) = {
    val host = args(0)
    val taskType = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    System.setProperty("spark.serializer", "spark.KryoSerializer")
    //System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.kryo.registrator", "spark.graphlab.AnalyticsKryoRegistrator")

    taskType match {
      case "pagerank" => {

        var numIter = Int.MaxValue
        var isDynamic = true
        var tol:Float = 0.001F
        var outFname = ""

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case ("tol", v) => tol = v.toFloat
          case ("output", v) => outFname = v
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          exit(1)
        }
        println("======================================")
        println("|             PageRank               |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        if(isDynamic) println(" \t  |-> Tolerance: " + tol)
        println(" \tNumIter:  " + numIter)
        println("======================================")

        val sc = new SparkContext(host, "PageRank(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => 1.0F)
        val startTime = System.currentTimeMillis
        val pr = if(isDynamic) Analytics.dynamicPageRank(graph, tol, numIter)
          else  Analytics.pageRank(graph, numIter)
        println("Total rank: " + pr.map(_._2).reduce(_+_))
        if(!outFname.isEmpty) {
          println("Saving pageranks of pages to " + outFname)
          pr.map(p => p._1 + "\t" + p._2).saveAsTextFile(outFname)
        }
        println("Runtime:    " + ((System.currentTimeMillis - startTime)/1000.0) + " seconds")
        sc.stop()
      }

     case "cc" => {

        var numIter = Int.MaxValue
        var isDynamic = true

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          exit(1)
        }
        println("======================================")
        println("|      Connected Components          |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        println(" \tNumIter:  " + numIter)
        println("======================================")

        val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => 1.0F)
        val cc = if(isDynamic) Analytics.dynamicConnectedComponents(graph, numIter)
          else  Analytics.connectedComponents(graph, numIter)
        println("Components: " + cc.map(_._2).distinct())

        sc.stop()
      }


     case "shortestpath" => {

        var numIter = Int.MaxValue
        var isDynamic = true
        var sources: List[Int] = List.empty

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case ("source", v) => sources ++= List(v.toInt)
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }


        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          exit(1)
        }

        if(sources.isEmpty) {
          println("No sources provided!")
          exit(1)
        }

        println("======================================")
        println("|          Shortest Path             |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        println(" \tNumIter:  " + numIter)
        println(" \tSources:  [" + sources.mkString(", ") + "]")
        println("======================================")

        val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => (if(a.isEmpty) 1.0F else a(0).toFloat ) )
        val cc = if(isDynamic) Analytics.dynamicShortestPath(graph, sources, numIter)
          else  Analytics.shortestPath(graph, sources, numIter)
        println("Longest Path: " + cc.map(_._2).reduce(math.max(_,_)))

        sc.stop()
      }


      case _ => {
        println("Invalid task type.")
      }
    }
  }






  //   println("One Iteration of PageRank on a large real graph")
  //   //    val graph = Graph.fromURL(sc, "http://parallel.ml.cmu.edu/share/google.tsv", a => true)
  //   val graph = Graph.textFile(sc, fname, a => true)
  //   println("\n\n\n\n\n\n")
  //   if(args.length > 2) {
  //     val numIter = args(2).toInt
  //     println("Running pagerank with numIter: " + numIter)
  //     val pr = Analytics.pageRank(graph, numIter)
  //     println("Total rank: " + pr.map(_._2).reduce(_+_))

  //   } else {
  //     println("Running Dynamic pagerank")
  //     val pr = Analytics.dynamicPageRank(graph)
  //     println("Total rank: " + pr.map(_._2).reduce(_+_))
  //   }
  // }

}
