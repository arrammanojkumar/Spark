package machineLearning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel

/**
 * @author marram
 */
object KMeansClustering {
  val sc = new SparkContext(new SparkConf().setAppName("KMeansClustering").setMaster("local"))

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage : <File Path> <Number of Clusters> <Number of interations> <Number of Runs> < Initialization mode 0 : Random 1. Parallel> (optional parameters are )<Train percentage> <Test percentage>")
    } else {
      /*    var filePath = "C://Users//marram//Desktop//normalizedf.csv"
            var numClusters = 2
            var numInterations = 4
            var runs = 1
            var initializationMode = 1
      
            var trainPercentage = 0.70
            var testPercentage = 0.30
       */

      var filePath = args(0)
      var numClusters = args(1).toInt
      var numInterations = args(2).toInt
      var runs = args(3).toInt
      var initializationMode = args(4).toInt

      var trainPercentage = 0.70
      var testPercentage = 0.30

      if (args.length == 7) {
        trainPercentage = args(5).toDouble
        testPercentage = args(6).toDouble
      }

      var dataRDD = getReadFile(filePath)

      var data_split = getSplittedDataset(dataRDD, trainPercentage, testPercentage)

      if (data_split != null) {
        var Array(trainData: RDD[Vector], testData: RDD[Vector]) = data_split
        trainData foreach println
        var trainModel = getTrainModel(trainData, numClusters, numInterations, runs, initializationMode)
        var testModel = getPredictedTestModel(trainModel, testData)
        var WSSE = getWSSE(trainModel, testData)
        print("Within Set Sum of Squared Errors : " + WSSE)
      } else {
        println("Check for Train and Test percentages ")
      }
    }
  }

  /**
   * Returns the file pointer handle
   */
  def getReadFile(filePath: String) = {
    var csvFile = sc.textFile(filePath)
    csvFile.map(line => Vectors.dense(line.split(",").map(_.toDouble)))
  }

  /**
   * Returns null if the train and test % are not valid
   * else returns an Array RDD with train and test data sets
   */
  def getSplittedDataset(dataRDD: RDD[Vector], trainPercentage: Double, testPercentage: Double) = {
    if (trainPercentage + testPercentage == 1.00)
      dataRDD.randomSplit(Array(trainPercentage, testPercentage))
    else
      null
  }

  /**
   * Returns a KMeansModel
   */
  def getTrainModel(dataset: RDD[Vector], numClusters: Int, numInterations: Int, runs: Int, initializationMode: Int) = {
    if (initializationMode == 0)
      KMeans.train(dataset, numClusters, numInterations, runs, KMeans.RANDOM)
    else
      KMeans.train(dataset, numClusters, numInterations, runs, KMeans.K_MEANS_PARALLEL)
  }

  /**
   * Returns cluster details
   */
  def getNumberOfClusters(kMeansModel: KMeansModel) = {
    kMeansModel.clusterCenters
  }

  /**
   * Returns the predicted RDD
   */
  def getPredictedTestModel(kMeansModel: KMeansModel, testDataset: RDD[Vector]) = {
    kMeansModel.predict(testDataset)
  }

  /**
   * Returns the Computed Cost i.e Within set sum of squared errors
   */
  def getWSSE(kMeansModel: KMeansModel, dataset: RDD[Vector]) = {
    kMeansModel.computeCost(dataset)
  }
}