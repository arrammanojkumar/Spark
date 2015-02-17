package sortNFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf);

		//Setting jar class
		job.setJarByClass(sortNFiles.SDriver.class);

		//Setting Mapper and Reducer class
		job.setMapperClass(sortNFiles.sMapper.class);
		job.setReducerClass(sortNFiles.sReducer.class);
		
		//Setting Input format
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		//Setting output Key format
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Adding the path to the job
		System.out.println("Input "+args[0]+"  and Output path "+args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Running the job
		job.waitForCompletion(true);
	}
}