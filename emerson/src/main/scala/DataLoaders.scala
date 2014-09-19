package edu.berkeley.emerson

import breeze.linalg.{max, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import edu.berkeley.emerson.Emerson.Params
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.util.hashing.MurmurHash3



object DataLoaders {
  def loadBismark(sc: SparkContext, filename: String, params: Params): RDD[Array[(Double, BV[Double])]] = {
    val data = sc.textFile(filename, params.numPartitions)
      .filter(s => !s.isEmpty && s(0) == '{')
      .map(s => s.split('\t'))
      .map {
      case Array(x, y) =>
        val features = x.stripPrefix("{").stripSuffix("}").split(',').map(xi => xi.toDouble)
        val label = if (y.toDouble > 0) 1.0 else 0.0
        val xFeatures: BV[Double] = new BDV[Double](features)
        (label, xFeatures)
    }.repartition(params.numPartitions).mapPartitions(iter => Iterator(iter.toArray)).cache()
    data
  }

  def makeDictionary(colId: Int, tbl: RDD[Array[String]]): Map[String, Int] = {
    tbl.map(row => row(colId)).distinct.collect.zipWithIndex.toMap
  }

  def makeBinary(value: String, dict: Map[String, Int]): Array[Double] = {
    val array = new Array[Double](dict.size)
    array(dict(value)) = 1.0
    array
  }

  def loadDBLP(sc: SparkContext, filename: String, params: Params): RDD[Array[(Double, BV[Double])]] = {
    println("loading data!")
    val rawData = sc.textFile(filename, params.numPartitions)

    var maxFeatureID = params.inputTokenHashKernelDimension
    if (params.inputTokenHashKernelDimension < 0) {
      maxFeatureID = rawData.map(line => max(line.split(' ').tail.map(s => s.toInt))).max()
    }

    rawData.map(line => {
      val splits = line.split(' ')
      val year = splits(0).toInt
      val label = if (year < params.dblpSplitYear) 0.0 else 1.0
      val features: Array[Double] = Array.fill[Double](maxFeatureID)(0.0)
      var i = 1
      while (i < splits.length) {
        val hc: Int = MurmurHash3.stringHash(splits(i))
        features(Math.abs(hc) % features.length) += (if (hc > 0) 1.0 else -1.0)
        i += 1
      }
      val x: BV[Double] = new BDV[Double](features)
      (label, x)
    }).repartition(params.numPartitions).mapPartitions(iter => Iterator(iter.toArray)).cache()
  }

  def loadWikipedia(sc: SparkContext, filename: String, params: Params):
      RDD[Array[(Double, BV[Double])]] = {
    println("loading data!")
    val rawData = sc.textFile(filename, params.numPartitions)

    var maxFeatureID = params.inputTokenHashKernelDimension
    if (params.inputTokenHashKernelDimension < 0) {
      maxFeatureID = rawData.map(line => max(line.split(' ').tail.map(s => s.toInt))).max()
    }

    val labelStr = params.wikipediaTargetWordToken.toString

    rawData.map(line => {
      val splits = line.split(' ')
      val features: Array[Double] = Array.fill[Double](maxFeatureID)(0.0)
      var i = 1
      var labelFound = false
      while (i < splits.length) {
        if(splits(i).equals(labelStr)) {
          labelFound = true
        } else {
          val hc: Int = MurmurHash3.stringHash(splits(i))
          features(Math.abs(hc) % features.length) += (if (hc > 0) 1.0 else -1.0)
        }
        i += 1
      }
      val label = if(labelFound) 1.0 else 0.0
      val x: BV[Double] = new BDV[Double](features)
      (label, x)
    }).repartition(params.numPartitions).mapPartitions(iter => Iterator(iter.toArray)).cache()
  }

  def loadFlights(sc: SparkContext, filename: String, params: Params):
      RDD[Array[(Double, BV[Double])]] = {
    val labels = Array("Year", "Month", "DayOfMonth", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime",
      "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
      "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
      "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay",
      "NASDelay", "SecurityDelay", "LateAircraftDelay").zipWithIndex.toMap
    println("Loading data")
    val rawData = sc.textFile(filename, params.numPartitions).
      filter(s => !s.contains("Year")).
      map(s => s.split(",")).cache()

    val carrierDict = makeDictionary(labels("UniqueCarrier"), rawData)
    val flightNumDict = makeDictionary(labels("FlightNum"), rawData)
    val tailNumDict = makeDictionary(labels("TailNum"), rawData)
    val originDict = makeDictionary(labels("Origin"), rawData)
    val destDict = makeDictionary(labels("Dest"), rawData)

    val data = rawData.map {
      row =>
        val value_arr = Array.fill(12)(1.0)
        val idx_arr = new Array[Int](12)

        var idx_offset = 0
        for(i <- 0 until 7 if i != 5) {
          value_arr(idx_offset) = if (row(i) == "NA") 0.0 else row(i).toDouble
          idx_arr(idx_offset) = idx_offset
          idx_offset += 1
        }

        var bitvector_offset = idx_offset
        idx_arr(idx_offset) = bitvector_offset + carrierDict(row(labels("UniqueCarrier")))
        idx_offset += 1
        bitvector_offset += carrierDict.size

        idx_arr(idx_offset) = bitvector_offset + flightNumDict(row(labels("FlightNum")))
        idx_offset += 1
        bitvector_offset += flightNumDict.size

        idx_arr(idx_offset) = bitvector_offset + tailNumDict(row(labels("TailNum")))
        idx_offset += 1
        bitvector_offset += tailNumDict.size

        idx_arr(idx_offset) = bitvector_offset + originDict(row(labels("Origin")))
        idx_offset += 1
        bitvector_offset += originDict.size

        idx_arr(idx_offset) = bitvector_offset + destDict(row(labels("Dest")))
        idx_offset += 1
        bitvector_offset += destDict.size

        // add one for bias term
        bitvector_offset += 1

        val delay = row(labels("ArrDelay"))
        val label = if (delay != "NA" && delay.toDouble > 0) 1.0 else 0.0

        assert(idx_offset == 11)
        val x: BV[Double] = new BSV[Double](idx_arr, value_arr, bitvector_offset)
        (label, x)
    }.repartition(params.numPartitions).mapPartitions(iter => Iterator(iter.toArray)).cache()


    data.count()
    rawData.unpersist(true)
    data
  }

  /*
    Build a dataset of points drawn from one of two 'point clouds' (one at [5,...] and one at [10, ...])
    with either all positive or all negative labels.

    labelNoise controls how many points within each cloud are mislabeled
    cloudSize controls the radius of each cloud
    partitionSkew controls how much of each cloud is visible to each partition
      partitionSkew = 0 means each partition only sees one cloud
      partitionSkew = .5 means each partition sees half of each cloud
   */
  def generatePairCloud(sc: SparkContext,
                        dim: Int,
                        labelNoise: Double,
                        cloudSize: Double,
                        partitionSkew: Double,
                        numPartitions: Int,
                        pointsPerPartition: Int): RDD[Array[(Double, BV[Double])]] = {
    sc.parallelize(1 to numPartitions, numPartitions).map { idx =>
      val plusCloud = new DenseVector(Array.fill[Double](dim)(10.0))
      plusCloud.values(dim - 1) = 1
      val negCloud = new DenseVector(Array.fill[Double](dim)(5.0))
      negCloud.values(dim - 1) = 1

      // Seed the generator with the partition index
      val random = new Random(idx)
      val isPartitionPlus = idx % 2 == 1

      (0 until pointsPerPartition).iterator.map { pt =>
        val isPointPlus = if (random.nextDouble() < partitionSkew) isPartitionPlus else !isPartitionPlus
        val trueLabel: Double = if (isPointPlus) 1.0 else 0.0

        val pointCenter = if (isPointPlus) plusCloud else negCloud

        // calculate the actual point in the cloud
        val chosenPoint: BV[Double] = BDV.zeros[Double](dim)
        for (d <- 0 until dim - 1) {
          chosenPoint(d) = pointCenter.values(d) + random.nextGaussian() * cloudSize
        }
        chosenPoint(dim - 1) = 1.0

        val chosenLabel: Double = if (random.nextDouble() < labelNoise) (trueLabel+1) % 2 else trueLabel

        (chosenLabel, chosenPoint)
      }.toArray
    }
  }
}
