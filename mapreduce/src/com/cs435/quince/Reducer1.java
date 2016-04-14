package com.cs435.quince;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] coordinates = key.toString().split(" ");
		double latitude = Double.parseDouble(coordinates[0]);
		double longitude = Double.parseDouble(coordinates[1]);

		for(Text value : values){
			String[] tmpValues = value.toString().split(" ");
			String timeStamp = tmpValue[0];
			double pmReading = Double.parseDouble(tmpValue[1]);
		}
		
		context.write(new Text(latitude + " " + longitude), new Text());
	}
}
