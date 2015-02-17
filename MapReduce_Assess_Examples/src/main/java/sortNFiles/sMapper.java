package sortNFiles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	public void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
	}
}
