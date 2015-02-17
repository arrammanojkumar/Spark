package sampleCombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text word, Iterable<IntWritable> values, Context context) {
		try {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			System.out.println(word + " -- " + sum);
			context.write(word, new IntWritable(sum));

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
