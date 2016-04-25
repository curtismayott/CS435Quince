package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, Text, Text> {
	int a;
	int b;
	String state;
	String county;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String row = (value.toString());
		String[] columns = row.split("\t");
		state = columns[0];
		county = columns[1];
		// get a
        	 a=Integer.parseInt(columns[2]);
        	 // get b
        	 b=Integer.parseInt(columns[3]);
        	context.write(new Text(), new Text( a + " " +  b ));
	}
}
