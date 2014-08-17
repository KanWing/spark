import java.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object MovieLensALS {

  def toRatings(data: RDD[(Int, Int, Double, Int)], bias: Double) = {
    data.map { 
      case (user, movie, rating, time) => Rating(user, movie, rating - bias)
    }
  }


  def makeDatasets2(sc: SparkContext) = {
    // load ratings and movie titles
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    val movieLensHomeDir = "hdfs://" + masterHostname + ":9000" 

    val rawTraining = sc.textFile(movieLensHomeDir + "/data/ra.train").map { line => 
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache


    val nTraining = rawTraining.count
    val biasTerm = rawTraining.map { x => x.rating }.reduce(_ + _) / nTraining.toDouble

    val training = rawTraining.map { x => Rating(x.user, x.product, x.rating - biasTerm) }.cache

    val trainingMovies = training.map { x => x.product }.collect.toSet
    val trainingMoviesBC = sc.broadcast(trainingMovies)

    val extendedTraining = sc.textFile(movieLensHomeDir + "/data/ra.train+seven").map { line => 
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - biasTerm)
    }.filter( r => trainingMoviesBC.value.contains(r.product) ).cache

    val test = sc.textFile(movieLensHomeDir + "/data/ra.three").map { line => 
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - biasTerm)
    }.filter( r => trainingMoviesBC.value.contains(r.product) ).cache

    println(s"Training:    ${training.count}")
    println(s"Training+:   ${extendedTraining.count}")
    println(s"Test:        ${test.count}")

    (biasTerm, training, extendedTraining, test)
  }


  def makeDatasets(sc: SparkContext) = {
    // load ratings and movie titles
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    val movieLensHomeDir = "hdfs://" + masterHostname + ":9000" 
    val trainingFile = movieLensHomeDir + "/data/ra.whole"

    val specialThreshold = 200
    val data = sc.textFile(trainingFile).map { line => 
      val Array(user, movie, rating, time) = line.split("::")
      Rating(user.toInt, movie.toInt, rating.toDouble)
    }.repartition(128).cache

    val specialUsers = data.filter(r => r.user < specialThreshold)
    val Array(specialTrainRaw, specialTestRaw) = specialUsers.randomSplit(Array(0.8, 0.2))

    println(s"nSpecialRaitings: ${specialUsers.count}")

    println(s"Data Length ${data.count}")

    val Array(trainingExtendedRaw, testRaw) = data.filter(r => r.user >= specialThreshold)
      .randomSplit(Array(0.95, 0.05))
    val Array(trainingRaw, _) = trainingExtendedRaw.randomSplit(Array(0.8, 0.2))
    
    val trainingExtendedRaw2 = trainingExtendedRaw.union(specialTrainRaw)
    val testRaw2 = testRaw.union(specialTestRaw)

 
    val nTraining = trainingRaw.count
    val biasTerm = trainingRaw.map { r => r.rating }.reduce(_ + _) / nTraining.toDouble

    val training = trainingRaw.map { r => Rating(r.user, r.product, r.rating - biasTerm) }.cache
    val trainingProducts = training.map { r => r.product }.collect.toSet
    val trainingProductsBC = sc.broadcast(trainingProducts)

    val trainingExtended = trainingExtendedRaw2.filter(r => trainingProductsBC.value.contains(r.product))
      .map { r => Rating(r.user, r.product, r.rating - biasTerm) }.cache

    val test = testRaw2.filter(r => trainingProductsBC.value.contains(r.product))
      .map { r => Rating(r.user, r.product, r.rating - biasTerm) }.cache

    val specialTest = specialTestRaw.filter(r => trainingProductsBC.value.contains(r.product))
      .map { r => Rating(r.user, r.product, r.rating - biasTerm) }.cache

    println(s"Training:    ${training.count}")
    println(s"Training+:   ${trainingExtended.count}")
    println(s"Test:        ${test.count}")


    (biasTerm, training, trainingExtended, test, specialTest)
  }



  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

   //  args.foreach(println(_))
    val numIter = args(0).toInt
    val rank    = args(1).toInt
    val lambda  = args(2).toDouble

    println(s"Num Iter:  $numIter")
    println(s"Rank:      $rank")
    println(s"lambda:    $lambda")


    val conf = new SparkConf().setAppName("Movie Lens Experiment")
    val sc = new SparkContext(conf)

    val (biasTerm, training, extendedTraining, test, specialTest) = makeDatasets(sc)
    val nSpecialTest = specialTest.count

    // val (biasTerm, training, extendedTraining, test) = makeDatasets2(sc)



    val nTrain = training.count

    val nTest = test.count


    val biasError = math.sqrt(test.map( r => r.rating * r.rating ).reduce(_ + _) / nTest.toDouble)

    println(s"Biase term:   ${biasTerm}")
    println(s"Naive Error:  ${biasError}")
    println(s"Num training: $nTrain")
 

    val (bestModel, bestLambda, bestRMSE) = crossValidate(extendedTraining, test, rank, numIter) 


    println("Training Base Model")
    val als = new ALS(-1, -1, rank, numIter, lambda, implicitPrefs = false, alpha = 0.0)
    val modelBase = als.run(training)

    println("Computing error on base model")
    val baselineError = computeRmse(modelBase, test, nTest)
    println(s"Baseline error: ${baselineError}")
    val baselineError2 = computeRmse(modelBase, specialTest, nSpecialTest)
    println(s"Special Test error: ${baselineError2}")



    // run for just one iteration skipping the products update
    val als2 = new ALS(als.numUserBlocks, als.numProductBlocks, rank, 1, lambda, false, 0.0)
    als2.skipProducts = true
    val model2 = als2.run(extendedTraining, modelBase.productFeatures)
    // Measure the new ratings
    val extendedError = computeRmse(model2, test, nTest)
    println(s"Extended error: ${extendedError}")

    val extendedError2 = computeRmse(model2, specialTest, nSpecialTest)
    println(s"Special Extended error: ${extendedError2}")


    val als3 = new ALS(-1, -1, rank, numIter, lambda,
      implicitPrefs = false, alpha = 0.0)
    val model3 = als3.run(extendedTraining, modelBase.productFeatures)
    // Measure the new ratings
    val batchError = computeRmse(model3, test, nTest)
    println(s"Extended Batch error: ${batchError}")

    val batchError2 = computeRmse(model3, specialTest, nSpecialTest)
    println(s"Special Extended Batch error: ${batchError2}")


    sc.stop()



    // val movies = sc.textFile(movieLensHomeDir + "/movies.dat").map { line =>
    //   val fields = line.split("::")
    //   // format: (movieId, movieName)
    //   (fields(0).toInt, fields(1))
    // }.collect.toMap

    // your code here

    // clean up
  }




  def crossValidate(train: RDD[Rating], validate: RDD[Rating], rankArg: Int, numIter: Int) = {
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestLambda = -1.0
    var bestNumIter = -1
    val lambdas = List(0.05,0.1, 0.15, 0.3)
    val ranks = List(5, 10, 15, 20, 25, 30, 50, 100)
    val numValidation = validate.count
    for (rank <- ranks) {
    for (lambda <- lambdas) {
      val model = ALS.train(train, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validate, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestLambda = lambda
      }
    }
    }

    println(s"Best lambda ${bestLambda}")
    (bestModel.get, bestLambda, bestValidationRmse)
  }


  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)), 0.0)
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
                                           .join(data.map(x => ((x.user, x.product), x.rating)))
                                           .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
  
}



    // val (bestModel, bestLambda, bestError) = crossValidate(train, test, rank = 25)
    // println(s"Baseline Error: ${baselineError}")
    // println(s"Biase term: ${biasTerm}")
    // println(s"Best Lambda: ${bestLambda}")
    // println(s"Best error: ${bestError}")

    // val trainExtendedFile = movieLensHomeDir + "/data/ra.train+seven"
    // println(s"Extended Training file: ${trainExtendedFile}")
    // val trainExtended = sc.textFile(trainExtendedFile).map { line =>
    //   val fields = line.split("::")
    //   // format: (timestamp % 10, Rating(userId, movieId, rating))
    //   Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - biasTerm)
    // }.cache
    // val nExtended = trainExtended.count

    // val missingProdIds = trainExtended.map { r => r.product }.subtract(train.map {r => r.product} )

    // println(s"Missing products ${missingProdIds.count}")


    // set up environment
    // val jarFile = "target/scala-2.10/movielens-als_2.10-0.0.jar"
    // val sparkHome = "/root/spark"
    // val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
    // val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    // val conf = new SparkConf()
    //   .setMaster(master)
    //   .setSparkHome(sparkHome)
    //   .setAppName("MovieLensALS")
    //   .set("spark.executor.memory", "8g")
    //   .setJars(Seq(jarFile))

    // // load ratings and movie titles
    // val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    // val movieLensHomeDir = "hdfs://" + masterHostname + ":9000" 


    // val data = sc.textFile("/data/ra.whole").map { line => 
    //   val Array(user, product, rating, time) = line.split("::")
    //   (user.toInt, product.toInt, rating.toDouble, (time.toInt % 10))
    // }.repartition(128).cache

    // val training = data.filter( x => x._4 < 5 )
    // val extend = data.filter(x => x._4 >= 5 & x._4 < 8)
    // val test = data.filter(x => x._4 >= 8)

    // val trainingUsers = training.map { x => x._1 }.collect.toSet
    // val trainingMovies = training.map { x => x._2 }.collect.toSet

    // val otherUsers = data.filter( x => x._4 >= 5).map{x => x._1}.collect.toSet
    // val otherMovies = data.filter( x => x._4 >= 5).map{x => x._2}.collect.toSet


    // val trainingFile = movieLensHomeDir + "/data/ra.train"
    // println(s"Training file: ${trainingFile}")
    // val trainRaw = sc.textFile(trainingFile).map { line =>
    //   val fields = line.split("::")
    //   // format: (timestamp % 10, Rating(userId, movieId, rating))
    //   Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    // }.cache

    // val biasTerm = trainRaw.map( r => r.rating ).reduce(_ + _) / nTrain.toDouble

    // val train = trainRaw.map( r => Rating(r.user, r.product, r.rating - biasTerm) ).cache

    // val testFile = movieLensHomeDir + "/data/ra.three"
    // println(s"Test file: ${testFile}")
    // val test = sc.textFile(testFile).map { line =>
    //   val fields = line.split("::")
    //   // format: (timestamp % 10, Rating(userId, movieId, rating))
    //   Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - biasTerm)
    // }.cache

