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

		int sumX = 0;
		int sumXY = 0;
		int sumY = 0;
		int sumX2 = 0;
		int size = 0;
		
		for(Text value : values){
			String[] tmpValues = value.toString().split(" ");
			String timeStamp = tmpValue[0];
			double pmReading = Double.parseDouble(tmpValue[1]);

			sumX += timeStamp;
			sumXY += pmReading * timeStamp;
			sumY += pmReading;
			sumX2 += timeStamp^2;
			size++;
		}
		// a = n * sum(x, y) - sum(x) * sum(y)
		// 	n * sum(x^2) - sum(x)^2
		int a = (size * sumXY - sumX * sumY) / (size * sumX2 - sumX^2);
		// b = (1/n) (sum(y)  - sum(x))
		int b = (1 / size) * (sumY - sumX);
		
		context.write(new Text(latitude + " " + longitude), new Text(a + " " + b));
	}
}
