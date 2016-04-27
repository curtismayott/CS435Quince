package com.cs435.quince;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] keys = key.toString().split("\t");
		String state = keys[0];
		String county = keys[1];

		double sumX = 0;
		double sumXY = 0;
		double sumY = 0;
		double sumX2 = 0;
		double size = 0;
		
		for(Text value : values){
			String[] tmpValues = value.toString().split("\t");
			int year = Integer.parseInt(tmpValues[0]);
			year = year - 1990;
			double pmReading = Double.parseDouble(tmpValues[1]);
			if(pmReading > 0){
				sumX += year;
				sumXY += pmReading * year;
				sumY += pmReading;
				sumX2 += year * year;
				size = size + 1.0;;
			}
		}
		// a = n * sum(x, y) - sum(x) * sum(y)
		// 	n * sum(x^2) - sum(x)^2
		try{
		double a = (size * sumXY - sumX * sumY) / (size * sumX2 - sumX * sumX);
		//int a = ((sumY * sumX2) - (sumX * sumXY)) / ((size * sumX2) - (sumX * sumX));

		// b = (1/n) (sum(y)  - sum(x))
		double b = (1 / size) * (sumY - sumX);
		//int b = ((size * sumXY) - (sumX * sumY)) / ((size * sumX2) - (sumX * sumX));
//System.out.println("sumX: " + sumX + " sumY: " + sumY + " sumXY: " + sumXY + " sumX2: " + sumX2 + " " + size);
		context.write(new Text(state + "\t" + county), new Text(a + "\t" + b));
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
