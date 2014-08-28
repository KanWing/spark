package edu.berkeley.emerson

import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.{Vector, Vectors}


/**
 * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent.
 * NOTE: Labels used in SVM should be {0, 1}.
 */
class SVMWithADMM(val params: ADMMParams) extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {

  override val optimizer: ADMM = new ADMM(params, new HingeLoss(), new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}


class SVMWithAsyncADMM(val params: ADMMParams) extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {

  override val optimizer = new AsyncADMM(params, new HingeLoss(), new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}


class SVMWithHOGWILD(val params: ADMMParams) extends GeneralizedLinearAlgorithm[SVMModel] with Serializable {

  override val optimizer = new HOGWILDSGD(params, new HingeLoss(), new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}


/**
 * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent.
 * NOTE: Labels used in SVM should be {0, 1}.
 */
class LRWithADMM(val params: ADMMParams)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  override val optimizer: ADMM = new ADMM(params, new LogisticLoss(), 
					  new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

class LRWithAsyncADMM(val params: ADMMParams)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  override val optimizer = new AsyncADMM(params, new LogisticLoss(), 
					 new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

class LRWithHOGWILD(val params: ADMMParams)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  override val optimizer = new HOGWILDSGD(params, new LogisticLoss(), 
					  new L2Regularizer())

  //override protected val validators = List(DataValidators.binaryLabelValidator)
  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}
