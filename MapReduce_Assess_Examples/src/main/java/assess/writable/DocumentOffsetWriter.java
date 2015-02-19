package assess.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentOffsetWriter implements Writable {

	/*
	 * String : Input File Name ; ArrayList : Position of a word
	 */
	private HashMap<String, ArrayList<Long>> map = new HashMap<String, ArrayList<Long>>();

	public DocumentOffsetWriter() {
	}

	public DocumentOffsetWriter(HashMap<String, ArrayList<Long>> map) {
		this.map = map;
	}

	/**
	 * Returns the Array list size
	 * 
	 * @param word
	 * @return
	 */
	public int getListCount(String word) {
		return map.get(word).size();
	}

	public String getPositions(String word) {
		return map.get(word).toString();
	}

	public String getCountPositions(String word) {
		return getListCount(word) + " " + getCountPositions(word);
	}

	public void write(DataOutput out) throws IOException {
		Iterator<String> it = map.keySet().iterator();

		while (it.hasNext()) {
			String t = it.next();
			new Text(t).write(out);
			new Text(getCountPositions(t)).write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		Iterator<String> it = map.keySet().iterator();
		Text tag = new Text();

		while (it.hasNext()) {
			String t = it.next();
			tag = new Text(t);
			tag.readFields(in);
			new Text(getCountPositions(t)).readFields(in);
		}

	}

	@Override
	public String toString() {
		String output = "";
		for (String tag : map.keySet()) {
			output += ("( " + tag + "=> WC : " + getListCount(tag) + " Pos : "
					+ getPositions(tag) + " )");
		}
		return output;
	}
}
