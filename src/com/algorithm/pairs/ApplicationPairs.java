/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.pairs;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class ApplicationPairs extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ApplicationPairs(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}
			Job job = Job.getInstance(new Configuration());
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(PairsMap.class);
			job.setReducerClass(PairsReducer.class);
			//job.setPartitionerClass(PairsPartitioner.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

			job.setJarByClass(ApplicationPairs.class);

			job.submit();
			return 0;
		}
	}