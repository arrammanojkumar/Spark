package machineLearning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors

/**
 * @author marram
 */
object KMeansClustering {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("KMeansClustering").setMaster("local"))
    var filePath = "C:/Users/marram/Desktop/Clustering Datasets/From Tools/ddsWithoutLables.csv"
    var csvFile = sc.textFile(filePath)

    /**
     * Reference for Cache http://spark.apache.org/docs/1.2.1/quick-start.html#caching
     */
    var dataRDD = csvFile.map(line => Vectors.dense(line.split(",").map(_.toDouble))).cache()

    val Array(f1,f2) = dataRDD.randomSplit(Array(0.70, 0.30))
    
    val numClusters = 3
    val numIterations = 40
    
    val kmeansModel = KMeans.train(f1, numClusters, numIterations,4)
    
    println("Cluster Centers are : ")
    kmeansModel.clusterCenters.foreach { x => println(x) }
    
    val testModel = kmeansModel.predict(f2)
    
    f2.foreach { x => println(x) }
    
    val WSSSETest = kmeansModel.computeCost(f2)
    val WSSSETrain = kmeansModel.computeCost(f1)
    
//    println("WSSE on Testing Data "+WSSSETest)
//    println("WSSE on Testing Data "+WSSSETrain)
//    testModel.foreach { x => println(x) }
  }
}