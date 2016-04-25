package com.cs435.quince;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] keys = key.toString().split(" ");
		double state = Double.parseDouble(keys[0]);
		double county = Double.parseDouble(keys[1]);

		int sumX = 0;
		int sumXY = 0;
		int sumY = 0;
		int sumX2 = 0;
		int size = 0;
		
		for(Text value : values){
			String[] tmpValues = value.toString().split(" ");
			int year = Integer.parseInt(tmpValues[0]);
			double pmReading = Double.parseDouble(tmpValues[1]);

			sumX += year;
			sumXY += pmReading * year;
			sumY += pmReading;
			sumX2 += year ^ 2;
			size++;
		}
		// a = n * sum(x, y) - sum(x) * sum(y)
		// 	n * sum(x^2) - sum(x)^2
		int a = (size * sumXY - sumX * sumY) / (size * sumX2 - sumX^2);
		// b = (1/n) (sum(y)  - sum(x))
		int b = (1 / size) * (sumY - sumX);
		
		context.write(new Text(state + " " + county), new Text(a + " " + b));
	}
}
