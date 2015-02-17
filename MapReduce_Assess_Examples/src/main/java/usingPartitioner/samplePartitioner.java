package usingPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class samplePartitioner extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numberOfTasks) {
		
		int Partition;
		int part = key.get();
		
		if ( part == -1 || numberOfTasks < 1)
			Partition = 0;
		
		if (part > 0 && part <= 1000)
			Partition = 0;
		else if (part > 1000 && part <= 3000)
			Partition= 1;
		else
			Partition=2;
		
		System.out.println("Key is "+part+" value : "+value+" Partition : "+Partition);

		return Partition;
	}
}
