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
	public static int currentYear = 2016;
	public static int oldestYear = 0;
	public static int predictionYear;
	public static String state;
	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
                        System.out.printf("Usage: ProcessLogs <input dir> <number years in future> <latitude start> <latitude end> <longitude start> <longitude end>\n");
                        System.exit(-1);
                }

// args 1 = num years
// args 2 = state
// args 3 = input dir
// args 4 = output dir

	        predictionYear = Integer.parseInt(args[1]);
		state = args[2];
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
		FileInputFormat.addInputPath(job1, new Path(args[3]));
		FileOutputFormat.setOutputPath(job1, new Path("/output/"));

		success = job1.waitForCompletion(true);
		if(success){
			Job job2 = Job.getInstance(conf, "quince2");
			job2.setJobName("quince2");
        	        job2.setJarByClass(Main.class);
                	job2.setOutputKeyClass(Text.class);
                	job2.setOutputValueClass(Text.class);
                	job2.setMapOutputKeyClass(Text.class);
                	job2.setMapOutputValueClass(Text.class);

                	job2.setMapperClass(Mapper2.class);
                	job2.setReducerClass(Reducer2.class);
			FileInputFormat.addInputPath(job1, new Path("/output/"));
			FileOutputFormat.setOutputPath(job1, new Path(args[4]));
			
			success = job2.waitForCompletion(true);
			if(!success){
				System.out.println("Error in Mapper2/Reducer2");
				System.exit(1);
			}
		}else{
			System.out.println("Error in Mapper1/Reducer1");
			System.exit(1);
		}
		System.exit(success ? 0 : 1);
	}
}
