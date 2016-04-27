package com.cs435.quince;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] columns = key.toString().split("\t");
		String state = columns[0];
		String county = columns[1];

		double y = 0;
		//int sumXY = 0;
		//int sumY = 0;
		//int sumX2 = 0;
		//int size = 0;
		
		for(Text value : values){
			String[] tmpValues = value.toString().split("\t");
			double a = Double.parseDouble(tmpValues[0]);
			double b = Double.parseDouble(tmpValues[1]);
			y = a * Main.predictionYear + b;
		}
		y = ((int)(y * 100.0)) / 100.0;
		context.write(new Text(state + " " + county), new Text(Double.toString(y)));
	}
}
