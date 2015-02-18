package assignments1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Prints the default value
 * 
 * @author Mani
 *
 */

public class FirstSpark {
	public static void main(String[] args) {
		// Setting app Name for the configuration
		SparkConf conf = new SparkConf().setMaster("local");

		conf.setAppName("Sample App");
		
		// Creating Spark Context
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		System.out.println(sc.appName());
		sc.stop();
	}
}