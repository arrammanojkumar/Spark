package usingPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UPMap extends Mapper<LongWritable, Text, IntWritable, Text> {

	public static Text word = new Text();

	@Override
	public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
//		Configuration conf=new Configuration(context.getConfiguration());

		try {
			StringTokenizer st = new StringTokenizer(line.toString());

			while (st.hasMoreTokens()) {
				int id = Integer.parseInt(st.nextToken());
				word.set(st.nextToken());
				
				System.out.println(" ID is "+id+" and value is "+word);
				context.write(new IntWritable(id), word);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
