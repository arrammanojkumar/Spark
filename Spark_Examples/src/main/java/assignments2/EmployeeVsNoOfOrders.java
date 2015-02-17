package assignments2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class EmployeeVsNoOfOrders {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(
				"Country Vs No of Customers").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> ordersRDD = spark
				.textFile("InputFiles_Assignment2//orders.csv");

		final String header = ordersRDD.first();

		/*
		 * This RDD does not have Header
		 */

		JavaRDD<String> ordersFilter = ordersRDD
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String v1) throws Exception {
						if (!v1.equals(header))
							return true;
						else
							return false;
					}
				});

		/*
		 * This method counts all the customers for each country
		 * 
		 * Key : Employee ID 
		 * Value : OrderID
		 */
		JavaPairRDD<Integer, Integer> eidOid = ordersFilter
				.mapToPair(new PairFunction<String, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, Integer> call(String t)
							throws Exception {
						String[] words = t.split("\\|");
						return new Tuple2<Integer, Integer>(Integer
								.parseInt(words[2]), 1);
					}
				});

		/*
		 * Reduce the no of Orders according to Employee
		 */
		JavaPairRDD<Integer, Integer> eidOidReduce = eidOid
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});

		JavaRDD<String> employeeDetails = spark
				.textFile("InputFiles_Assignment2//Employees.csv");

		// Header
		final String headerEmployees = employeeDetails.first();

		/*
		 * This RDD does not have Header
		 */
		JavaRDD<String> eidEname = employeeDetails
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String v1) throws Exception {
						if (!v1.equals(headerEmployees))
							return true;
						else
							return false;
					}

				});

		/*
		 * eidEnameDetails has 
		 * Key : Employee Id 
		 * Value : Employee Name
		 */
		JavaPairRDD<Integer, String> eidEnameDetails = eidEname
				.mapToPair(new PairFunction<String, Integer, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(String t)
							throws Exception {
						String[] words = t.split("\\|");
						return new Tuple2<Integer, String>(Integer
								.parseInt(words[0]),
								(words[1] + " " + words[2]));
					}
				});

		JavaPairRDD<Integer, Tuple2<String,Integer>> joinRDD= eidEnameDetails.join(eidOidReduce);
		
		//Result is 
		System.out.println(joinRDD.values().collect());
		
		spark.stop();
		spark.close();
	}
}
