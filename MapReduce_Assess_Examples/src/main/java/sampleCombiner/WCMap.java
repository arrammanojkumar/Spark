package sampleCombiner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static final IntWritable one = new IntWritable(1);
	public static Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		StringTokenizer tokens = new StringTokenizer(line);

		while (tokens.hasMoreElements()) {
			word.set(tokens.nextToken());
			context.write(word, one);
		}
		System.out.println("In Map : "+word);
	}
}
