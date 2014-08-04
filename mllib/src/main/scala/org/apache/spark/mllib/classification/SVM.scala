/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.classification

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.rdd.RDD

/**
 * Model for Support Vector Machines (SVMs).
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class SVMModel (
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable {

  private var threshold: Option[Double] = Some(0.0)

  /**
   * :: Experimental ::
   * Sets the threshold that separates positive predictions from negative predictions. An example
   * with prediction score greater than or equal to this threshold is identified as an positive,
   * and negative otherwise. The default value is 0.0.
   */
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
   * :: Experimental ::
   * Clears the threshold so that `predict` will output raw prediction scores.
   */
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double) = {
    val margin = weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
    threshold match {
      case Some(t) => if (margin < t) 0.0 else 1.0
      case None => margin
    }
  }

  override protected def pointLoss(
      point: LabeledPoint,
      weightMatrix: Vector,
      intercept: Double) = {
    val LabeledPoint(y, x) = point
    val wdotx = weightMatrix.toBreeze.dot(x.toBreeze) + intercept
    val svmLabel = 2 * y - 1.0
    math.max(0.0, 1.0 - svmLabel * wdotx)
  }
}

/**
 * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent.
 * NOTE: Labels used in SVM should be {0, 1}.
 */
class SVMWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {

  private val gradient = new HingeGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a SVM object with default parameters
   */
  def this() = this(1.0, 100, 1.0, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}

/**
 * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent.
 * NOTE: Labels used in SVM should be {0, 1}.
 */
class SVMWithADMM extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {
  var stepSize: Double = 1.0
  var maxGlobalIterations: Int = Int.MaxValue
  var maxLocalIterations: Int = Int.MaxValue
  var regParam: Double = 1.0
  var miniBatchFraction: Double = 2.0
  var localEpsilon: Double = 1.0e-5
  var epsilon: Double = 1.0e-5
  var updater: Updater = new SquaredL2Updater()
  var collectLocalStats: Boolean = true


  private val gradient = new HingeGradient()
  private val localSolver = new SGDLocalOptimizer(gradient, updater)

  override val optimizer = new ADMM(localSolver)

  def setup() {
    localSolver.eta_0 = stepSize
    localSolver.maxIterations = maxLocalIterations
    localSolver.epsilon = epsilon
    localSolver.miniBatchFraction = miniBatchFraction
    localSolver.collectStats = collectLocalStats
    optimizer.displayLocalStats = collectLocalStats
    optimizer.numIterations = maxGlobalIterations
    optimizer.regParam = regParam
    optimizer.epsilon = localEpsilon
  }

  override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}

class SVMWithAsyncADMM extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {
  var epsilon: Double = 1.0e-5
  var updater: Updater = new SquaredL2Updater()
  var totalSeconds = 10
  var numberOfParamBroadcasts = 10
  var regParam: Double = 1.0
  var miniBatchFraction: Double = 0.1

  private val gradient = new HingeGradient()
  override val optimizer = new AsyncADMMwithSGD(gradient, updater)

  def setup() {
    optimizer.totalSeconds = totalSeconds
    optimizer.numberOfParamBroadcasts = numberOfParamBroadcasts
    optimizer.regParam = regParam
    optimizer.miniBatchFraction = miniBatchFraction
  }

  override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}


/**
 * Top-level methods for calling SVM. NOTE: Labels used in SVM should be {0, 1}.
 */
object SVMWithSGD {

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   *
   * NOTE: Labels used in SVM should be {0, 1}.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): SVMModel = {
    new SVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double): SVMModel = {
    new SVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double): SVMModel = {
    train(input, numIterations, stepSize, regParam, 1.0)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   */
  def train(input: RDD[LabeledPoint], numIterations: Int): SVMModel = {
    train(input, numIterations, 1.0, 1.0, 1.0)
  }
}
