package org.agonar.mrp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class mrpReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String outputs="";
		int count=0;
		for (Text value : values ){

			outputs+=value + " ";
			count++;
		}
		context.write(new Text(key+"\t"+count), new Text(outputs+""));

	}
}