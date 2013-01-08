package spark.graphlab

import Array._

import org.scalatest.{ FunSuite, Assertions, BeforeAndAfter }
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import scala.collection.mutable.ArrayBuffer

import spark._
import spark.SparkContext
import spark.SparkContext._

import spark.graphlab.Graph._

class GraphLabSuite extends FunSuite with Assertions with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    if (sc == null) {
      sc = new SparkContext("local[1]", "test")
    }
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("GraphCreation") {
    val graph = Graph.ballAndChain(sc)
    val numEdges = graph.numEdges
    val numVertices = graph.numVertices
    assert(numEdges == numVertices)
  }

  test("SingleConnectedComponent:Dynamic") {
    println("[Dyanmic] Testing Single Connected Components")
    val graph = Graph.ballAndChain(sc)
    val ccId = Analytics.dynamicConnectedComponents(graph)
    val all1 = ccId.map(_._2 == 1).reduce(_ && _)
    assert(all1)
  }

  test("KCycleConnectedComponents:Dynamic") {
    println("[Dynamic] Testing K Connected Components")
    val graph = Graph.kCycles(sc, 1000, 10)
    val ccId = Analytics.dynamicConnectedComponents(graph)
    val allAgree = ccId.join(graph.vertices).map {
      case (vid, (ccId, origId)) => ccId == origId
    }.reduce(_ && _)
    assert(allAgree)
  }



  test("LazySingleConnectedComponent:Static") {
    println("[Static] Testing Single Connected Components")
    val graph = Graph.ballAndChain(sc)
    println("Making solution RDD")
    val ccId = Analytics.connectedComponents(graph, 5)
    println("Solution RDD constructed")
    val all1 = ccId.map(_._2 == 1).reduce(_ && _)
    assert(all1)
  }


  test("LazyKCycleConnectedComponents:Static") {
    println("[Static] Testing K Connected Components")
    val graph = Graph.kCycles(sc, 1000, 10)
    println("Making solution RDD")
    val ccId = Analytics.connectedComponents(graph, 5)
    println("Solution RDD constructed")
    val allAgree = ccId.join(graph.vertices).map {
      case (vid, (ccId, origId)) => ccId == origId
    }.reduce(_ && _)
    assert(allAgree)
  }

  test("LazyKCycleConnectedComponents2:Static") {
    println("Testing K Connected Components")
    val graph = Graph.kCycles(sc, 1000, 10)
    println("Making solution RDD (for wrong iteration count)")
    val ccId = Analytics.connectedComponents(graph, 2)
    println("Solution RDD constructed")
    val allAgree = ccId.join(graph.vertices).map {
      case (vid, (ccId, origId)) => ccId == origId
    }.reduce(_ && _)
    assert(!allAgree)
  }

  test("PageRankKCycles:Static") {
    println("Testing PageRank")
    val graph = Graph.kCycles(sc, 100, 10)
    val edges = graph.edges.collect()
    val outDegree = graph.edges.map { case (src, target, data) => (src, 1)}.reduceByKey(_ + _).collect().toMap
    val maxVid = outDegree.keys.max

    println(maxVid)

    var pr = Array.fill(maxVid+1)(1.0F)
    for(i <- 0 until 5) {
      val prOld = pr
      pr = Array.fill(maxVid+1)(0.15F)
      edges.foreach{ case (src, dst, _) => pr(dst) += 0.85F * prOld(src) / outDegree(src) }
    }

    val glpr = Analytics.pageRank(graph, 5).collect()

    glpr.foreach { case (vid, value) => assert( math.abs(pr(vid) - value) < 1.0E-5 ) }
  }

  test("PageRankGrid:Static") {
    println("Testing PageRank")
    val graph = Graph.grid(sc, 10, 10)


    val edges = graph.edges.collect()
    val outDegree = graph.edges.map { case (src, target, data) => (src, 1)}.reduceByKey(_ + _).collect().toMap
    val maxVid = graph.vertices.map(v => v._1).reduce(math.max(_, _))


    var pr = Array.fill(maxVid+1)(1.0F)
    for(i <- 0 until 10) {
      val prOld = pr
      pr = Array.fill(maxVid+1)(0.15F)
      edges.foreach{ case (src, dst, _) => pr(dst) += 0.85F * prOld(src) / outDegree(src) }
    }

    val glpr = Analytics.pageRank(graph, 10).collect()
    glpr.foreach { case (vid, value) => assert( math.abs(pr(vid) - value) < 1.0E-5 ) }

  }


//
//   test("GooglePageRank") {
//    println("One Iteration of PageRank on a large real graph")
////    val graph = Graph.fromURL(sc, "http://parallel.ml.cmu.edu/share/google.tsv", a => true)
//    val graph = Graph.textFile(sc, "/Users/jegonzal/Data/google.tsv", a => true)
//    val pr = Analytics.pageRank(graph, 1)
//  }


}
