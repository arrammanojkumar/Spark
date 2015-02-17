package sampleCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WCDriver {
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
		try {
			
			Configuration config = new Configuration();
			Job job = new Job(config, "Map1");

			job.setJarByClass(WCDriver.class);

			System.out.println("Set Jar Done ");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			System.out.println("Set Output Done ");
			
			job.setMapperClass(WCMap.class);
			
			//Adding Combiner Class
			job.setCombinerClass(WCCombiner.class);
			job.setReducerClass(Reduce.class);
			
			System.out.println("Setting Mapper Combiner and Reducer");			
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			System.out.println("Set I/O Format Done "+args[0]+" and "+args[1]);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.out.println("Waiting for Completion");
			job.waitForCompletion(true);
			
			System.out.println("Total Done ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
