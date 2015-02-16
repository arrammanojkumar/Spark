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
 * This Program is to know Number of products for each category
 * 
 * @author Manoj Kumar
 *
 */
public class CategoryVsNoOfProducts {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(
				"Number of prodcts for Each Category").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> productsRDD = spark
				.textFile("InputFiles_Assignment2//products.csv");

		final String header = productsRDD.first();

		JavaRDD<String> productFiltersRDD = productsRDD
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
		 * Product Category RDD has 
		 * Key : Category Id 
		 * Value : product
		 */
		JavaPairRDD<Integer, Integer> ProductCategory = productFiltersRDD
				.mapToPair(new PairFunction<String, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, Integer> call(String t)
							throws Exception {
						String[] words = t.split("\\|");
						return new Tuple2<Integer, Integer>(Integer
								.parseInt(words[3]), 1);
					}
				});

		/*
		 * Reduce the no of products according to their category
		 */
		JavaPairRDD<Integer, Integer> productReduceRDD = ProductCategory.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		JavaRDD<String> categoriesRDD = spark
				.textFile("InputFiles_Assignment2//categories.csv");

		//Header 
		final String headerCategories = categoriesRDD.first();

		/*
		 * This RDD does not have Header
		 */
		JavaRDD<String> categoriesFilterRDD = categoriesRDD
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String v1) throws Exception {
						if (!v1.equals(headerCategories))
							return true;
						else
							return false;
					}

				});

		/*
		 * CategoryDetailsRDD has Key : Category Id Value : Category Name
		 */
		JavaPairRDD<Integer, String> CategoryDetailsRDD = categoriesFilterRDD
				.mapToPair(new PairFunction<String, Integer, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(String t)
							throws Exception {
						String[] words = t.split("\\|");
						return new Tuple2<Integer, String>(Integer
								.parseInt(words[0]), words[1]);
					}
				});

		JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = CategoryDetailsRDD.join(productReduceRDD);

		System.out.println(joinRDD.collect());
		
		JavaRDD<Tuple2<String, Integer>> result = joinRDD.values();
		
		System.out.println(result.collect());
		
		spark.stop();
		spark.close();
	}
}
