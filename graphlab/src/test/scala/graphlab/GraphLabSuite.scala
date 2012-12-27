package spark.graphlab

import org.scalatest.{FunSuite, Assertions, BeforeAndAfter}
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import scala.collection.mutable.ArrayBuffer

import spark._

import spark.graphlab.Graph._



class GraphLabSuite extends FunSuite with Assertions with BeforeAndAfter {

  
  var sc: SparkContext = _
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }
  
  test("graph_loading") {
    sc = new SparkContext("local[4]", "test")
    val graph = Graph.ballAndChain(sc)
    val numEdges = graph.numEdges
    val numVertices = graph.numVertices
    assert(numEdges == numVertices)
    
  }


}
