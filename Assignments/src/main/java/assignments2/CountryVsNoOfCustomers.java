package assignments2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Aim : To display for each country how many no of customers we have.
 * 
 * @author Manoj Kumar
 *
 */
public class CountryVsNoOfCustomers {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(
				"Country Vs No of Customers").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = spark
				.textFile("InputFiles_Assignment2//customers.csv");

		final String header = fileRDD.first();

		/*
		 * This RDD does not have Header
		 */
		JavaRDD<String> filtedRDD = fileRDD
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String v1) throws Exception {
						if (!v1.equals(header))
							return true;
						else
							return false;
					}

				});

		/**
		 * This is the method for Map to each Country and Customer..
		 * 
		 * Key : Country Value : Customer
		 * 
		 */
		JavaPairRDD<String, Integer> mapRDD = filtedRDD
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String t)
							throws Exception {
						String words[] = t.split("\\|");
						return new Tuple2<String, Integer>(words[6], 1);
					}
				});

		/**
		 * This method counts all the customers for each country
		 * 
		 * Key : Country Value : Customer
		 * 
		 */

		JavaPairRDD<String, Integer> result = mapRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});

		// Printing The result
		System.out.println(result.collect());

		spark.stop();
		spark.close();

	}
}
