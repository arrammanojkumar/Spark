import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
 /** 
  *  @author marram
 */
object Sample {
  def main(args: Array[String]): Unit = {
    println("Hello");
    var conf =new SparkConf().setMaster("local");
    conf.setAppName("Sample App");
    
    // Creating Spark Context
    var sc = new SparkContext(conf);

    println(sc.appName);
    sc.stop();    
  }
}