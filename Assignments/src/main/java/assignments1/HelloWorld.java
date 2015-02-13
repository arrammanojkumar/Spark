package assignments1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloWorld {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Hello World !!! ").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println(sc.appName()+" is my First App Name ");
		
		sc.stop();
		sc.close();
	}
}
