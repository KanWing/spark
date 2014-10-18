package org.apache.spark.graphx.lib

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Correlation clustering identifies subgroups of connected vertices ....
 */
object CorrelationClustering {

  // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
  def hash64shift(keyInput: Long): Long = {
    var key = keyInput
    key = (~key) + (key << 21) // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24)
    key = (key + (key << 3)) + (key << 8) // key * 265
    key = key ^ (key >>> 14)
    key = (key + (key << 2)) + (key << 4) // key * 21
    key = key ^ (key >>> 28)
    key + (key << 31)
  }

  def vertexBatch(id: VertexId, numBatches: Int): Int = {
    (hash64shift(id) % numBatches).toInt
  }


  def biDirectGraph(graph: Graph[_, _]): Graph[Long, Byte] = {
    // big shuffle
    val biDirectedEdges = graph.edges
      .map { // Orient edges in canonical direction
      e => if (e.srcId < e.dstId) (e.srcId, e.dstId) else (e.dstId, e.srcId)
      }
      .distinct() // Reduce all edges to one direction
      .flatMap(e => Iterator(Edge(e._1, e._2, 1.toByte), Edge(e._2, e._1, 1.toByte)))

    // Make the graph
    val initialVertexValue = 0L
    Graph.fromEdges(biDirectedEdges, initialVertexValue)
  }


  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Long, Byte] = {
    // Enumerate the veritces and bidirect the edges
    var g = biDirectGraph(graph)

    // Loop over the batches
    val numBatches = 10
    for ( batch <- 0 until numBatches) {
      // Get the subgraph of vertices in this batch
      val activeVertices = g.vertices.filter { case (id, component) =>
        (vertexBatch(id, numBatches) == batch) && component == 0
      }

      // Send message out of the active vertices
      val msgs = g.mapReduceTriplets[Long](
        triplet => {
          if (triplet.srcId < triplet.dstId) {
            Iterator((triplet.dstId, triplet.srcAttr))
          } else {
            Iterator.empty[(VertexId, Long)]
          }
        },
        (a,b) => math.min(a,b),
        Some(activeVertices, EdgeDirection.Out))

      // Identify the set of active vertices without neighbors in this round
      val changingVertices = activeVertices.leftJoin(msgs) { (id, component, msgOption) =>
        msgOption.isEmpty
      }.filter { case (id, noMsg) => noMsg }.cache

      // Set the cluster component for those verices in the graph
      g = g.joinVertices(changingVertices) { (id, component, noMsg) =>
        if (noMsg) id else component
      }.cache()

      // Send messages notifying neigbhors of values
      val newValues = g.mapReduceTriplets[Long](
        triplet => {
          if (triplet.dstAttr > 0 && triplet.srcId > triplet.dstId) {
            Iterator((triplet.dstId, triplet.srcAttr))
          } else {
            Iterator.empty[(VertexId, Long)]
          }
        },
        (a,b) => math.min(a,b),
        Some(changingVertices, EdgeDirection.Out))


      // Assign the each vertex the new value
      g = g.joinVertices(newValues) { (id, component, newValue) =>
        math.min(component, newValue) // this seems wrong since they are already negative
      }.cache()

    }

    g
  } // end of connectedComponents

}
