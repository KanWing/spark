package spark.graphlab

import spark._
import spark.SparkContext._

object Analytics {

  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def pageRank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], maxIter: Int = 10) = {
    // Compute the out degree of each vertex
    val outDegree = graph.edges.flatMap {
      case (src, target, data) => Array((src, 1), (target, 0))
    }.reduceByKey(_ + _)

    // Construct the pagerank graph with
    //   vertex data: (Degree, Rank, OldRank)
    //   edge data: None
    val vertices = outDegree.mapValues(deg => (deg, 1.0F, 1.0F))
    val edges = graph.edges //.map(v => None)
    val pageRankGraph = new Graph(vertices, edges)
    // Run PageRank
    pageRankGraph.iterate(
      (me_id, edge) => {
        // val Edge(Vertex(_, (degree, rank, _)), _, edata) = edge
        // rank / degree
        edge.source.data._2 / edge.source.data._1
      }, // gather
      (a: Float, b: Float) => a + b, // merge
      0F,
      (vertex, a: Float) => {
        // val Vertex(vid, (out_degree, rank, old_rank)) = vertex
        // (out_degree, (0.15F + 0.85F * a), rank)
        (vertex.data._1, (0.15F + 0.85F * a), vertex.data._2)
      }, // apply
      (me_id, edge) => {
        // val Edge(Vertex(_, (_, new_rank, old_rank)), _, edata) = edge
        // math.abs(new_rank - old_rank) > 0.01
        math.abs(edge.source.data._2 - edge.source.data._1) > 0.01
      }, // scatter
      maxIter).vertices.mapValues { case (degree, rank, oldRank) => rank }
    //    println("Computed graph: #edges: " + graph_ret.numEdges + "  #vertices" + graph_ret.numVertices)
    //    graph_ret.vertices.take(10).foreach(println)
  }

  /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def connectedComponents[VD: Manifest, ED: Manifest](graph: Graph[VD, ED]) = {

    val vertices = graph.vertices.mapPartitions(iter => iter.map { case (vid, _) => (vid, vid) })
    val edges = graph.edges // .mapValues(v => None)
    val ccGraph = new Graph(vertices, edges)

    val niterations = Int.MaxValue
    ccGraph.iterate(
      (me_id, edge) => edge.otherVertex(me_id).data, // gather
      (a: Int, b: Int) => math.min(a, b), // merge
      Integer.MAX_VALUE,
      (v, a: Int) => math.min(v.data, a), // apply
      (me_id, edge) => edge.otherVertex(me_id).data > edge.vertex(me_id).data, // scatter
      niterations,
      gatherEdges = EdgeDirection.Both,
      scatterEdges = EdgeDirection.Both).vertices
    //
    //    graph_ret.vertices.collect.foreach(println)
    //    graph_ret.edges.take(10).foreach(println)
  }

  def main(args: Array[String]) = {
    val host = args(0)
    val fname = args(1)

    //System.setProperty("spark.serializer", "spark.KryoSerializer")
    //System.setProperty("spark.kryo.registrator", "mypackage.MyRegistrator")

    val sc = new SparkContext(host, "Analytics")
    println("One Iteration of PageRank on a large real graph")
    //    val graph = Graph.fromURL(sc, "http://parallel.ml.cmu.edu/share/google.tsv", a => true)
    val graph = Graph.textFile(sc, fname, a => true)
    val pr = Analytics.pageRank(graph, 1)
    println("Total rank: " + pr.map(_._2).reduce(_+_))
  }

}
