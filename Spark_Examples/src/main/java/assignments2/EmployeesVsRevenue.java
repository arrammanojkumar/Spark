package assignments2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class EmployeesVsRevenue {
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(
				"Number of prodcts for Each Category").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		/************ Products Part *************************/

		JavaRDD<String> products = spark
				.textFile("InputFiles_Assignment2//products.csv");

		// Store Header
		final String header = products.first();

		// Remove Header
		JavaRDD<String> productFiltersRDD = products
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
		 * For each product store its respective Price
		 */
		final JavaPairRDD<Integer, Double> pidPrice = productFiltersRDD
				.mapToPair(new PairFunction<String, Integer, Double>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, Double> call(String t)
							throws Exception {
						String words[] = t.split("\\|");
						return new Tuple2<Integer, Double>(Integer
								.parseInt(words[0]), Double
								.parseDouble(words[5]));
					}

				});

		pidPrice.persist(new StorageLevel());

		/************ Orders Part *************************/

		JavaRDD<String> orders = spark
				.textFile("InputFiles_Assignment2//order details.csv");

		// Store Header
		final String headerOrder = orders.first();

		// Remove Header
		JavaRDD<String> orderFilterRDD = orders
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String v1) throws Exception {
						if (!v1.equals(headerOrder))
							return true;
						else
							return false;
					}

				});

		/*
		 * Key : pid
		 * Value : OID
		 */
		JavaPairRDD<Integer, Integer> oidPid = orderFilterRDD.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Integer> call(String t) throws Exception {
				String words[] = t.split("\\|");
				
				return new Tuple2<Integer, Integer>(Integer.parseInt(words[2]), Integer.parseInt(words[1]));
			}
			
		});
		
		/*
		 * Key : 
		 */
		JavaPairRDD<Integer, Integer> oidQty = orderFilterRDD.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Integer> call(String t) throws Exception {
				String words[] = t.split("\\|");
				
				return new Tuple2<Integer, Integer>(Integer.parseInt(words[1]), Integer.parseInt(words[3]));
			}
			
		});

		JavaPairRDD<Integer, Tuple2<Double, Integer>> jOidPidPrice = pidPrice.join(oidPid);
		System.out.println(jOidPidPrice.collect());
		
		spark.stop();
		spark.close();
	}
}
