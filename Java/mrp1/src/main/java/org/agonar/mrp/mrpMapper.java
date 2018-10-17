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
		
		//leave alphanums, urls and hashtags
        String filtered = status.getText().toLowerCase().replaceAll("[^A-Za-z0-9\'#//:\\./g]+", " ");        
        String msg[] = filtered.split(" ");
        
        String words[] = {"trump","flu","zika","diarrhea","ebola","headache","measles"};
             
        for(int i=0; i<msg.length;i++) {
        	for(int j=0; j<words.length;j++) {
        		if(msg[i].equalsIgnoreCase(words[j])) {
        			context.write(new Text(words[j]), new IntWritable(1));
        		}
        	}     	
        }
    }
}