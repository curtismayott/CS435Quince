package com.cs435.quince;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;
import java.util.Collections;

public class Main {
	public static double predictionYear;
	public static int numberOfFolds = 4;
	public static String state;
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
                        System.out.printf("Usage: ProcessLogs <number years in future> <state> <input dir> <output dir>\n");
                        System.exit(-1);
                }

// args 1 = num years
// args 2 = state
// args 3 = input dir
// args 4 = output dir
for(int i = 0; i < args.length; i++){
System.out.println(args[i]);
}
	        predictionYear = Double.parseDouble(args[0]) + 25.0;
	
		state = args[1];
		Configuration conf = new Configuration();
                boolean success = false;
                Job job1 = Job.getInstance(conf, "quince1");
                job1.setJobName("quince1");
                job1.setJarByClass(Main.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);
                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);
		//job1.setInputFormatClass(WholeFileInputFormat.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		FileInputFormat.addInputPath(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, new Path("/output/"));

		success = job1.waitForCompletion(true);

		System.exit(success ? 0 : 1);
	}
}
