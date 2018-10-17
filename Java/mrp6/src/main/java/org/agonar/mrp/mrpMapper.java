package org.agonar.mrp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class mrpMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String rawJSON = value.toString();
		Status status = null;
		try {
			status = TwitterObjectFactory.createStatus(rawJSON);
		} catch (Exception e) {
			e.printStackTrace();
		}

		String username=status.getUser().getScreenName();
		String msg=status.getText();

		context.write(new Text(username+""), new Text(msg));

	}
}