package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	String latitude;
	String longitude;
	String pmReading;
	String timeStamp;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String fullText = value.toString();
		String lines = fullText.split("\n");
		for(String row : lines){
			// get latitude

			// get longitude
			
			// get PM2.5 reading
			
			// get timestamp
			// add oldest year
			
			context.write(new Text(latitude + " " + longitude), new Text(timeStamp + " " + pmReading));
		}
	}
}
