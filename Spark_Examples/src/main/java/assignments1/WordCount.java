package assignments1;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Word Count ").setMaster(
				"local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = spark.textFile("D://Git Hub//Spark//Assignments//input.txt");

		
		/**
		 * It splits each sentence into words
		 */
		JavaRDD<String> wordsRDD = fileRDD
				.flatMap(new FlatMapFunction<String, String>() {

					private static final long serialVersionUID = -3749724865158701574L;

					public Iterable<String> call(String t) throws Exception {
						return Arrays.asList(t.split(" "));
					}
					
				});
		
		/**
		 * For each word it assigns 1 adn returns respective tuple
		 */
		JavaPairRDD<String, Integer> mapRDD = wordsRDD
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String,Integer>(t,1);
						
					}

				});

		/**
		 * For each key it takes each tuple as input and it adds respective counts 
		 */
		JavaPairRDD<String,Integer> count = mapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		System.out.println("Word Counts are \n : "+count.collect());
		
		spark.stop();
		spark.close();
	}
}
