package edu.berkeley.emerson

import breeze.linalg.{max, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import edu.berkeley.emerson.Emerson.Params
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util.MLUtils
import scala.collection.mutable.ArrayBuffer

import scala.util.Random
import scala.util.hashing.MurmurHash3


object RandomAccessDataset {
  def apply(entries: Array[(Double, BV[Double])]): RandomAccessDataset = {
    new LegacyDataset(entries)
  }
  def apply(ndim: Int, labels: Array[Double], offsets: Array[Int], featureIds: Array[Int], 
  featureValues: Array[Double]): RandomAccessDataset = {
    new SparseDataset(ndim, labels, offsets, featureIds, featureValues)
  }
}

abstract class RandomAccessDataset extends Serializable {
  def apply(ind: Int): (Double, BV[Double])
  def size(): Int
  def length() = size()

  def sumx(): BV[Double] = {
    var accum = apply(0)._2.copy
    var i = 1
    while (i < size()) {
      accum += apply(i)._2
      i += 1
    }
    accum
  }

  def sumxx(): BV[Double] = {
    var accum = apply(0)._2.copy
    var i = 1
    while (i < size()) {
      accum *= apply(i)._2
      i += 1
    }
    accum
  }

  def foldLeft[Z](zero: Z)(f: (Z, (Double, BV[Double]))  => Z): Z = {
    var accum = zero
    var i = 1
    while (i < size()) {
      accum = f(accum, apply(i))
      i += 1
    }
    accum
  }

  def iterator(): Iterator[(Double, BV[Double])] = {
    new Iterator[(Double, BV[Double])] {
      var i = 0
      override def hasNext() = i < size
      override def next() = {
        val ret = apply(i)
        i += 1
        ret
      }
    }
  }

  def normalize(xbar: BV[Double], stdev: BV[Double]): RandomAccessDataset = {
    null
  }
}

class LegacyDataset(entries: Array[(Double, BV[Double])]) extends RandomAccessDataset {
  override def apply(ind: Int) = entries(ind)
  override def size() = entries.size
  override def normalize(xbar: BV[Double], stdev: BV[Double]): RandomAccessDataset = {
    val newEntries = entries.map { case (y, x) =>
      assert(x.size == stdev.size)
      // x -= xbar
      // x /= stdev
      // x /= xmax

      // ugly hack where I reuse the vector
      var i = 0
      while (i < x.size) {
        if (stdev(i) == 0.0) {
          x(i) = 1.0 // make a constant column
        } else {
          x(i) = (x(i) - xbar(i)) / stdev(i)
        }
        // x(i) /= xmax(i)
        i += 1
      }

      (y, x)
    }
    RandomAccessDataset(newEntries)
  }
  
}

// class DenseDataset(labels: Array[Double], values: Array[Double]) extends RandomAccessDataset {
//   override def apply(ind: Int) = entries(ind)
// }


class SparseDataset(ndim: Int, labels: Array[Double], offsets: Array[Int], featureIds: Array[Int], 
  featureValues: Array[Double]) extends RandomAccessDataset {
  override def apply(ind: Int) = {
    val label = labels(ind)
    val begin = offsets(ind)
    val end = offsets(ind + 1)
    assert(begin <= end)
    val idx = featureIds.view(begin, end)
    val xValues = featureValues.view(begin, end)
    val x: BV[Double] = new BSV[Double](idx.toArray, xValues.toArray, ndim)
    (label, x)
  }
  override def size() = labels.size
}



object DataLoaders {

  def loadLibSVM(sc: SparkContext, filename: String, params: Params): RDD[RandomAccessDataset] = {
    val partitioned = sc.textFile(params.input)
      // .sample(false, 0.01).cache
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .coalesce(params.numPartitions)
    val counters = partitioned.mapPartitions { iter =>
      var features = 0L
      var labels = 0
      while (iter.hasNext) {
        val line = iter.next
        labels += 1
        features += line.count(_ == ':')
      }
      assert(features < Int.MaxValue)
      Iterator((labels, features.toInt))
    }

    val parsed = partitioned.zipPartitions(counters) { (iter, countersIter) =>
      val (nLabels, nFeatures) = countersIter.next
      val labels = new Array[Double](nLabels)
      val offsets = new Array[Int](nLabels + 1)
      val featureIds = new Array[Int](nFeatures)
      val values = new Array[Double](nFeatures)

      var labelsInd = 0
      var featuresInd = 0
      while (iter.hasNext) {
        val line = iter.next
        val items = line.split(' ')
        val label = items.head.toFloat
        labels(labelsInd) = if (label > 0) 1.0 else 0.0
        offsets(labelsInd) = featuresInd
        labelsInd += 1

        var i = 1
        while (i < items.size) {
          if (items(i).nonEmpty) {
            val indexAndValue = items(i).split(':')
            // Convert 1-based indices to 0-based.
            featureIds(featuresInd) = indexAndValue(0).toInt - 1
            values(featuresInd) = indexAndValue(1).toDouble
            featuresInd += 1
          }
          i += 1
        }
      }
      offsets(labelsInd) = featuresInd
      assert(labelsInd == (offsets.size - 1))
      assert(featuresInd == featureIds.size)

      Iterator((labels.toArray, offsets.toArray, featureIds.toArray, values.toArray))
      }.cache()

    val numDimensions = 
      parsed.map { case (labels, offsets, featureIds, values) => featureIds.max + 1 }
        .reduce((a,b) => math.max(a,b))

    println(s"Libsvm numDim: $numDimensions")
   
    parsed.map { case (labels, offsets, featureIds, values) =>
      RandomAccessDataset(numDimensions, labels, offsets, featureIds, values) }
    
  }

  def loadBismark(sc: SparkContext, filename: String, params: Params): RDD[RandomAccessDataset] = {
    val data = sc.textFile(filename, params.numPartitions)
      .filter(s => !s.isEmpty && s(0) == '{')
      .map(s => s.split('\t'))
      .map {
      case Array(x, y) =>
        val features = x.stripPrefix("{").stripSuffix("}").split(',').map(xi => xi.toDouble)
        val label = if (y.toDouble > 0) 1.0 else 0.0
        val xFeatures: BV[Double] = new BDV[Double](features)
        (label, xFeatures)
    }.repartition(params.numPartitions).mapPartitions(iter => Iterator(RandomAccessDataset(iter.toArray))).cache()
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

  def loadDBLP(sc: SparkContext, filename: String, params: Params): RDD[RandomAccessDataset] = {
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
        // shit, so we want to subtract, i think, no?
        // this is just the vector... yep but I want to keep the sign in my scaling code
        features(Math.abs(hc) % features.length) += (if (hc > 0) 1.0 else -1.0)
        i += 1
      }
      val x: BV[Double] = new BDV[Double](features)
      (label, x)
    }).repartition(params.numPartitions).mapPartitions(iter => Iterator(RandomAccessDataset(iter.toArray))).cache()
  }

  def loadWikipedia(sc: SparkContext, filename: String, params: Params): RDD[RandomAccessDataset] = {
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
    }).repartition(params.numPartitions).mapPartitions(iter => Iterator(RandomAccessDataset(iter.toArray))).cache()
  }

  private def hrMinToScaledHr(s: String): Double = {
    var r = s.toDouble
    val mins = r % 60
    (((r-mins)/100*60)+mins)/60/24
  }

  def loadFlights(sc: SparkContext, filename: String, params: Params): RDD[RandomAccessDataset] = {
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
        val value_arr = Array.fill(10)(1.0)
        val idx_arr = new Array[Int](10)

        var idx_offset = 0
        for(i <- 1 until 5) {
          var v: Double = 0

          if (row(i) != "NA") {
            v = row(i).toDouble

            // month
            if (i == 1) {
              v /= 12
            }

            // day of month
            if (i == 2) {
              v /= 31
            }

            // day of week
            if (i == 3) {
              v /= 7
            }

            // deptime
            if (i == 4) {
              v = hrMinToScaledHr(row(i))
            }
          }

          value_arr(idx_offset) = v
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

      /*
        idx_arr(idx_offset) = bitvector_offset + tailNumDict(row(labels("TailNum")))
        idx_offset += 1
        bitvector_offset += tailNumDict.size
        */

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

        val x: BV[Double] = new BSV[Double](idx_arr, value_arr, bitvector_offset)
        (label, x)
    }.repartition(params.numPartitions).mapPartitions(iter => Iterator(RandomAccessDataset(iter.toArray))).cache()

    data.count()
    rawData.unpersist(true)

    // val nData = data.map(_.length).reduce(_ + _)
    // val dimInterval = data.map(_.map { case (y, x) => Interval(x.size) }.reduce(_+_)).reduce(_ + _) / nData.toDouble
    // val maxValue = data.map(_.map { case (y, x) => Interval(x.max) }.reduce(_+_)).reduce(_+_) / nData.toDouble
    // val minValue = data.map(_.map { case (y, x) => Interval(x.min) }.reduce(_+_)).reduce(_+_) / nData.toDouble
    // println(s"nData:        $nData")
    // println(s"dimInterval:  $dimInterval")
    // println(s"maxValue:     $maxValue")
    // println(s"minValue:     $minValue")

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
                        pointsPerPartition: Int): RDD[RandomAccessDataset] = {
    sc.parallelize(1 to numPartitions, numPartitions).map { idx =>
      val plusCloud = new DenseVector(Array.fill[Double](dim)(5.0))
      plusCloud.values(dim - 1) = 1
      val negCloud = new DenseVector(Array.fill[Double](dim)(0.0))
      negCloud.values(dim - 1) = 1

      // Seed the generator with the partition index
      val random = new Random(idx)
      val isPartitionPlus = idx % 2 == 1

      val array =  (0 until pointsPerPartition).iterator.map { pt =>
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
      RandomAccessDataset(array)
    }
  }


  def elementMax(a: BV[Double], b: BV[Double]): BV[Double] = {
    val res = a.toDenseVector
    var i = 0
    val n = a.length
    while (i < n) {
      res(i) = math.max(a(i), b(i))
      i += 1
    }
    res
  }


  def elementMin(a: BV[Double], b: BV[Double]): BV[Double] = {
    val res = a.toDenseVector
    var i = 0
    val n = a.length
    while (i < n) {
      res(i) = math.min(a(i), b(i))
      i += 1
    }
    res
  }


  def normalizeData(data: RDD[RandomAccessDataset]): RDD[RandomAccessDataset] = {
    val nExamples = data.map { data => data.length }.reduce( _ + _ )

    // val xmax: BV[Double] = data.map {
    //   data => data.view.map { case (y, x) => x }.reduce((a,b) => elementMax(a,b))
    // }.reduce((a,b) => elementMax(a,b))

    // val xmin: BV[Double] = data.map {
    //   data => data.view.map { case (y, x) => x }.reduce((a,b) => elementMin(a,b))
    // }.reduce((a,b) => elementMin(a,b))


    val xbar: BV[Double] = data.map {
      data => data.sumx()
    }.reduce(_ + _) / nExamples.toDouble

    val xxbar: BV[Double] = data.map {
      data => data.sumxx()
    }.reduce(_ + _) / nExamples.toDouble

    val variance: BDV[Double] = (xxbar - (xbar :* xbar)).toDenseVector

    val stdev: BDV[Double] = breeze.numerics.sqrt(variance)

    assert(xbar.size == stdev.size)

    // val res = (xmin.toArray, stdev.toArray, xmax.toArray).zipped.toArray
    // println(res.mkString(", ")) 
    // println(s"Standard deviation: $stdev")

    // // Just in case there are constant columns set the standard deviation to 1.0
    // for(i <- 0 until stdev.size) {
    //   if (stdev(i) == 0.0) { stdev(i) = 1.0 }
    // }

    // // Just in case there are constant columns set the standard deviation to 1.0
    // for(i <- 0 until xmax.size) {
    //   if (xmax(i) == 0.0) { xmax(i) = 1.0 }
    // }

    val data2 = data.map { data => data.normalize(xbar, stdev) }.cache()
    data2.foreach( x => () )
    data.unpersist()
    data2
  }

}
