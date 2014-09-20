package edu.berkeley.emerson

import java.util.concurrent.TimeUnit

import breeze.linalg.{norm, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

object DualDecomp {
  // Super ugly hack to copy the primal var to each of the workers without reallocating.
  def copyPrimalVars(src: Array[BV[Double]], dst: Array[BV[Double]]): Unit = {
    var i = 0
    while (i < src.length) {
      var j = 0
      while (j < dst.length) {
        dst(i)(j) = src(i)(j)
        j += 1
      }
      i += 1
    }
  }
}

class DualDecomp extends BasicEmersonOptimizer with Serializable with Logging {

  var iteration = 0
  var solvers: RDD[DualDecompLocalOptimizer] = null
  var stats: Stats = null
  var totalTimeMs: Long = -1

  def statsMap(): Map[String, String] = {
    Map(
      "iterations" -> stats.avgLocalIters().x.toString,
      "iterInterval" -> stats.avgLocalIters().toString,
      "avgSGDIters" -> stats.avgSGDIters().toString,
      "avgMsgsSent" -> stats.avgMsgsSent().toString,
      "avgMsgsRcvd" -> stats.avgMsgsRcvd().toString,
      "primalAvgNorm" -> norm(stats.primalAvg(), 2).toString,
      "dualAvgNorm" -> norm(stats.dualAvg(), 2).toString,
      "consensusNorm" -> norm(finalW, 2).toString,
      "dualUpdates" -> stats.avgDualUpdates.toString,
      "runtime" -> totalTimeMs.toString,
      "stats" -> stats.toString
    )
  }

//  def setup(rawData: RDD[(Double, Vector)], initialWeights: Vector) {
//    val primal0 = initialWeights.toBreeze
//    val nSubProblems = rawData.partitions.length
//    val nData = rawData.count
//    solvers =
//      rawData.mapPartitionsWithIndex { (ind, iter) =>
//        val data: Array[(Double, BV[Double])] = iter.map {
//          case (label, features) => (label, features.toBreeze)
//        }.toArray
//        val solver = new DualDecompLocalOptimizer(ind, nSubProblems = nSubProblems,
//          nData = nData.toInt, data = data,
//          lossFunction = lossFunction, regularizer = regularizer,
//          params = params, primal0 = primal0.copy)
//        // Initialize the primal variable and primal regularizer
//        Iterator(solver)
//      }.cache()
//      solvers.count
//
//    // rawData.unpersist(true)
//    solvers.foreach( f => System.gc() )
//  }


  var finalW: BV[Double] = null

  /**
   * Solve the provided convex optimization problem.
   */
  override def optimize(): BV[Double] = {
    // Initialize the solvers
    val primal0 = initialWeights
    solvers = data.mapPartitionsWithIndex { (ind, iter) =>
      val data: Array[(Double, BV[Double])] = iter.next()
      val solver = new DualDecompLocalOptimizer(ind, nSubProblems = nSubProblems,
        nData = nData.toInt, data = data,
        lossFunction = lossFunction, regularizer = regularizationFunction,
        params = params, primal0 = primal0.copy)
      // Initialize the primal variable and primal regularizer
      Iterator(solver)
    }.cache()


    var primalResidual = Double.MaxValue
    var dualResidual = Double.MaxValue

    var primalVars = Array.fill(nSubProblems)(initialWeights.copy)

    val starttime = System.currentTimeMillis()
    val startTimeNs = System.nanoTime()

    var rho = params.rho0

    iteration = 0

    while (iteration < params.maxIterations &&
      (primalResidual > params.tol || dualResidual > params.tol) &&
      (System.currentTimeMillis() - starttime) < params.runtimeMS ) {
      println("========================================================")
      println(s"Starting iteration $iteration.")

      val timeRemaining = params.runtimeMS - (System.currentTimeMillis() - starttime)
      // Run the local solvers
      val (primalVarMap, localStats) = solvers.map { solver =>
        // Make sure that the local solver did not reset!
        assert(solver.localIters == iteration)
        solver.localIters += 1

        // "Broadcast the primal vars to all the machines"
        DualDecomp.copyPrimalVars(primalVars, solver.primalVars)

        // Do a dual update
        solver.dualUpdate(solver.params.lagrangianRho)

        // Do a primal update
        solver.primalUpdate(Math.min(timeRemaining, params.localTimeout))

        // Construct stats
        (List((solver.subProblemId -> solver.localPrimalVar)), solver.getStats())
      }.reduce( (a, b) => (a._1 ++ b._1 , a._2 + b._2)  )
      stats = localStats
      // update the primal vars
      primalVarMap.foreach { case (i, primalVar) => primalVars(i) = primalVar }

      println(s"Iteration: $iteration")
      println(stats)
      // println(s"(Primal Resid, Dual Resid, Rho): $primalResidual, \t $dualResidual, \t $rho")
      iteration += 1
    }

    val totalTimeNs = System.nanoTime() - startTimeNs
    totalTimeMs = TimeUnit.MILLISECONDS.convert(totalTimeNs, TimeUnit.NANOSECONDS)

    println("Finished!!!!!!!!!!!!!!!!!!!!!!!")

    finalW = stats.primalAvg()
    finalW

  }

}

