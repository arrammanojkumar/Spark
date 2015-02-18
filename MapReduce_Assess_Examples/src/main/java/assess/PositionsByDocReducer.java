package assess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import assess.writable.DocumentOffsetWriter;

public class PositionsByDocReducer extends
		Reducer<Text, Text, Text, DocumentOffsetWriter> {
	HashMap<String, ArrayList<Long>> map;

	public void add(String fileName, String offset) {
		ArrayList<Long> offsetList;

		if (map.get(fileName) != null) {
			offsetList = map.get(fileName);
			map.remove(fileName);
		} else {
			offsetList = new ArrayList<Long>();
			offsetList.add(Long.parseLong(offset));
		}

		offsetList.add(Long.parseLong(offset));

		map.put(fileName, offsetList);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		map = new HashMap<String, ArrayList<Long>>();

		System.out.println("Reduce key " + key + "  values : \t");

		for (Text value : values) {
			System.out.print(value +" ");
			
			String fileName = value.toString().split("\\@")[0];
			String offset = value.toString().split("\\@")[1];
			
			add(fileName, offset);
		}

		context.write(key, new DocumentOffsetWriter(map));
	}
}
