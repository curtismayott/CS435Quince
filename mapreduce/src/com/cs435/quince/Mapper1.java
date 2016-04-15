package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	double latitude;
	double longitude;
	double pmReading;
	String timeStamp;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String fullText = value.toString();
		String lines = fullText.split("\n");
		for(String row : lines){
			
			String[] columns = row.split(",");
			// get latitude
			latitude = Double.parseDouble(columns[5]);

			// get longitude
			longitude = Double.parseDouble(columns[6]);
			
			// get PM2.5 reading
			pmReading = Double.parseDouble(columns[16]);
			
			// get timestamp
			timeStamp = columns[11];

			// add oldest year
			
			context.write(new Text(Double.toString(latitude) + " " + Double.toString(longitude)), new Text(timeStamp + " " + Double.toString(pmReading)));
		}
	}
}
