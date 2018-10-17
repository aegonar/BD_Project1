package org.agonar.mrp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class mrpReducer extends Reducer<Text, LongWritable, Text, Text> {

	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		String outputs="";
		for (LongWritable value : values ){

			outputs+=value + " ";
		}
		context.write(key, new Text(outputs+""));

	}
}