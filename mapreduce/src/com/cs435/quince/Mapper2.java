package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {
	double a;
	double b;
	String state;
	String county;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String row = (value.toString());
		String[] columns = row.split("\t");
		state = columns[0];
		county = columns[1];
		// get a
        	 a = Double.parseDouble(columns[2]);
        	 // get b
        	 b = Double.parseDouble(columns[3]);
        	context.write(new Text(state + "\t" + county), new Text( a + "\t" +  b ));
	}
}
