package assess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PositionsByDocMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text value, Context context)
			throws IOException, InterruptedException {

		// Retrieving original file name from which this input split came
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

		Long currentOffset = getOffset(context, offset);

		// perform operation regarding text splitting and getting word and all
		StringTokenizer token = new StringTokenizer(value.toString(), ", . ; : \t \r \n \f");

		// System.out.println("Offset : " + currentOffset);

		// For space and special character counting
		Long index = 0L;
		int wordLenth = 0;
		while (token.hasMoreTokens()) {

			String word = token.nextToken().replaceAll("[^\\w\\s]", "").trim();

			// Removing Null Words
			if (word.length() > 0) {
				/*
				 * Checking weather it is first line in input file or not
				 * 
				 * if TRUE : don't add 1 to index ( not adding beacuse of '\n'
				 * character )
				 * 
				 * else : add 1 to index
				 */
				if (currentOffset > 0L)
					index = value.toString().indexOf(word, wordLenth)
							+ currentOffset;
				else
					index = value.toString().indexOf(word, wordLenth)
							+ currentOffset + 1;

				wordLenth += word.length();

				// System.out.println(" context ==>  "
				// + (fileName + "@" + currentOffset) + " Index is : "
				// + index);

				context.write(new Text(word.toLowerCase()), new Text(fileName
						+ "@" + index));
			}
		}
	}

	/**
	 * This method is for retrieving the offset
	 * 
	 * @param context
	 * @param offset
	 * @return
	 */
	public Long getOffset(Context context, LongWritable offset) {
		// Getting the offset of this line from original file
		String[] splitDetails = context.getInputSplit().toString().split("\\:");

		// Retrieve starting position of offset
		Long splitStartOffset, currentOffset = Long.MIN_VALUE;

		if (splitDetails.length == 4) {
			// System.out.println("Offset Details : " + splitDetails[3]);

			String offsetDetails[] = splitDetails[3].split("\\+");
			splitStartOffset = Long.parseLong(offsetDetails[0]);

			currentOffset = Long.parseLong(offset.toString())
					+ splitStartOffset;
		} else {
			System.out.println("Some error with get split check  "
					+ context.getInputSplit().toString());
		}
		return currentOffset;
	}
}
