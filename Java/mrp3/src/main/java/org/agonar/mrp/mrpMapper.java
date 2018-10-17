package org.agonar.mrp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class mrpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String rawJSON = value.toString();
		Status status = null;
		try {
			status = TwitterObjectFactory.createStatus(rawJSON);
		} catch (Exception e) {
			e.printStackTrace();
		}

		context.write(new Text(status.getUser().getScreenName()+""), new IntWritable(1));

	}
}