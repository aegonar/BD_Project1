package org.agonar.mrp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class mrpMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String rawJSON = value.toString();
		Status status = null;
		try {
			status = TwitterObjectFactory.createStatus(rawJSON);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long replyId=status.getInReplyToStatusId();

		if(replyId != -1) {
			context.write(new Text(replyId+""), new LongWritable(status.getId()));
		}

	}
}