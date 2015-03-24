package assess;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

public class PositionsByDocReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	
	HashMap<String, JSONArray> map;

	@SuppressWarnings("unchecked")
	public void add(String fileName, String offset) {
		JSONArray offsetList;

		if (map.get(fileName) != null) {
			offsetList = map.get(fileName);
		} else {
			offsetList = new JSONArray();
		}

		offsetList.add(Long.parseLong(offset));

		map.put(fileName, offsetList);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		map = new HashMap<String, JSONArray>();

		for (Text value : values) {

			String fileName = value.toString().split("\\@")[0];
			String offset = value.toString().split("\\@")[1];

			add(fileName, offset);
		}
		
		JSONObject hashMapJson = new JSONObject(map);
		
		JSONObject toWrite = new JSONObject();

		try {
			toWrite.put("word", key.toString());
			toWrite.put("docs", hashMapJson);

			context.write(NullWritable.get(), new Text(toWrite.toString()));
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
