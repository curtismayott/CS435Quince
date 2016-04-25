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
	String pmReading;
	String year;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String row = value.toString();
		String[] columns = row.split(",");
		state = columns[21];
		if(!row.contains("State Code") && state.equals(Main.state)){
			county = columns[22];
			
			// get PM2.5 reading
			pmReading =columns[13];
			
			// get timestamp
			year = ((columns[9]).split("-"))[0];
			
			// add oldest year
			
			context.write(new Text(state + " " + county), new Text(year + " " + pmReading));
		}
		else{ 
			if(row.contains("State Code")){ 
				System.out.println(row);
			}
		}
	}
}
