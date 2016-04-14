package com.cs435.quince;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private String word = "";
        private String author = "";
        int sum = 0;
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] keySplit = key.toString().split("\t");
                context.write(new Text(author + "\t" + word), new Text(Double.toString(termFrequency)));
        }
}
