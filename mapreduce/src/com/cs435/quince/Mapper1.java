package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {
	String state;
	String county;
	String pmReading;
	String yearMonth;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String row = value.toString();
		row = row.replaceAll("\"", "");
		String[] columns = row.split(",");
		state = columns[21];
		if(!row.contains("State Code") && state.equals(Main.state)){
			county = columns[22];
			// get PM2.5 reading
			pmReading =columns[13];
			// get timestamp
			yearMonth = ((columns[9]).split("-"))[0] + "-" + ((columns[9]).split("-"))[1];
			
			//System.out.println(state + " " + county + " " + pmReading + " " + year);
			context.write(new Text(state + "\t" + county), new Text(yearMonth + "\t" + pmReading));
		}
		else{ 
			//System.out.println(row);
		}
	}
}
