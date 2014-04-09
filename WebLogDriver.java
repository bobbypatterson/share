/*
 * Copyright (c) 2014, Optimetrum LLC. All Rights Reserved.
 * 
 * This is a driver class that sets up and runs a map-only job to parse and filter web log entries. It requires
 * command line parameters for the input and output directories in HDFS
 */
package com.optimetrum.share;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebLogDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new WebLogDriver(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		// check for required command line input 
		if (args.length != 2) {
			System.out.printf("Usage: WebLogDriver <input dir> <output dir>\n");
			return -1;
		}

		// Set up Job
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(WebLogDriver.class);
		job.setJobName("Web Log Filter and Cleanup");

		// Set input and output paths from command line arguments
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WebLogMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the number of reduce tasks to 0 since this is a map-only job.
		job.setNumReduceTasks(0);

		// Run job and check for success
		boolean success = job.waitForCompletion(true);
		if (success) {
			return 0;
		} else {
			return 1;
		}	
	}

}

