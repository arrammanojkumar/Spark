package assess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PositionsByDocDriver {
	public static void main(String[] args) {

		try {
			if (args.length != 2) {
				System.out
						.println("Usage hadoop jar <path to jar File> <Driver Class name > <Input file path> <Output File path> \n NOTE : INPUT AND OUTPUT FILES SHOULD BE IN HDFS ");
			} else {
				Configuration conf = new Configuration();
				Job job = new Job(conf, "");

				job.setJarByClass(PositionsByDocDriver.class);
				System.out.println("Set jar done");

				job.setMapperClass(PositionsByDocMapper.class);
				job.setReducerClass(PositionsByDocReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				System.out.println("Set Mapper and Reducer done");

				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(Text.class);
				System.out.println("Settign key and values done ");

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				System.out.println("Set I/O Format Done " + args[0] + " and "
						+ args[1]);

				// give folder path as input path
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				System.out.println("Waiting for Completion");
				job.waitForCompletion(true);

				System.out.println("Total Done ");

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
