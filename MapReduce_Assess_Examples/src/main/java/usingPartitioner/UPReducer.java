package usingPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UPReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		
			for (Text value : values) {
				System.out.println("Key " + key + " value " + value);
				context.write(key, value);
			}
	}
}
