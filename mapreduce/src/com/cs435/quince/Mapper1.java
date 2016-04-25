package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	String state;
	String county;
	double pmReading;
	String year;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String row = value.toString();
		String[] columns = row.split(",");
		// get latitude
		county = columns[5];

		// get longitude
		state = columns[6];
		
		// get PM2.5 reading
		pmReading = Double.parseDouble(columns[16]);
		
		// get timestamp
		year = columns[11];

		// add oldest year
			
		context.write(new Text(state + " " + county), new Text(year + " " + Double.toString(pmReading)));
	}
}
