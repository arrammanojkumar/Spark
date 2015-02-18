package assignments1;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class LogRead {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		SparkConf conf = new SparkConf();
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> file = sc.textFile("F://Innominds//Learning//Spark//Spark1//log.txt");
		file.persist(new StorageLevel());
		
		JavaRDD<String> php = file.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) throws Exception {
				return s.contains("ERROR") && s.contains("php");
			}
		});
		
		JavaRDD<String> mySql = file.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) throws Exception {
				return s.contains("ERROR") && s.contains("mysql");
			}
		});
		
		System.out.println("PHP collects "+php.collect()+" count is "+php.count());
		System.out.println("My collects  "+mySql.collect()+" count is "+mySql.count());
		
		ArrayList<String> list = new ArrayList<String>();
		
		list.add("PHP collects "+php.collect()+" count is "+php.count());
		list.add("My collects  "+mySql.collect()+" count is "+mySql.count());

		JavaRDD<String> fileOutput = sc.parallelize(list);
		
		fileOutput.saveAsTextFile("F://Innominds//Learning//Spark//OutPut");
		
		sc.stop();
		//sc.close();
	}
}
