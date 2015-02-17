package usingPartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UPDriver {
	public static void main(String[] args) {
		try {
			
			Configuration conf = new Configuration();
			Job job = new Job(conf);

			// Changing the number of reducer tasks
			job.setNumReduceTasks(3);
			System.out.println("Changing the number of reducer tasks");

			// Setting the Main Driver class
			job.setJarByClass(usingPartitioner.UPDriver.class);

			// Setting input and output formats
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// Configuring the partitioner
			job.setPartitionerClass(samplePartitioner.class);
			
			// Configure the Mapper and Reducer classes
			job.setMapperClass(UPMap.class);
			job.setReducerClass(UPReducer.class);
			
			// Setting output key and values format
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			// Setting Map output key and values format
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			
			// Adding the path to the job
			System.out.println("Input "+args[0]+"  and Output path "+args[1]);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// Running the job
			System.out.println("Waiting");
			job.waitForCompletion(true);
			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
