package com.cs435.quince; 

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String author;
        private String text;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String fullText = value.toString();
		word.set(keyToWrite);
		context.write(new Text(keyToWrite), new Text());
	}
}
