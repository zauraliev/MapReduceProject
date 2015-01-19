package com.algorithm.hybrid;

/**
 * @author ZAUR ALIYEV
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HybridApproach extends Configured implements Tool {
	public static class ParisStripesMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable count = new IntWritable(1);
		private Text word = new Text();

		/**
		 * Mapper method used Pair approach
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			ArrayList<String> list = GetLineAsList(line);

			for (int i = 0; i < list.size(); i++) {
				for (int j = i + 1; j < list.size(); j++) {

					if (list.get(i).equals(list.get(j)))
						break;

					String pair = list.get(i) + "," + list.get(j);
					word.set(pair);
					context.write(word, count);
				}
			}
		}

		private ArrayList<String> GetLineAsList(String line) {
			ArrayList<String> sentList = new ArrayList<String>();
			String[] lines = line.split(" ");
			for (String s : lines) {
				sentList.add(s);
			}

			return sentList;
		}

	}

	/**
	 * Reducer method used Stripe approach
	 */

	public static class ParisStripesReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		private long mTotal = 0;
		private String mPrevLeftKey = null;
		private Map<String, Integer> mMap = new TreeMap();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0;
			String currLeftKey = key.toString().split(",")[0];
			String rightKey = key.toString().split(",")[1];

			if (mPrevLeftKey != null && !mPrevLeftKey.equals(currLeftKey)) {

				flush(context);

				// reset intermediate result
				mTotal = 0;
				mPrevLeftKey = currLeftKey;
				mMap = new HashMap();
			}

			for (IntWritable val : values) {

				Integer oldV = mMap.get(rightKey);

				if (oldV != null) {
					mMap.put(rightKey, val.get() + oldV);
				} else {
					mMap.put(rightKey, val.get());
				}

				sum += val.get();
			}

			mPrevLeftKey = currLeftKey;
			mTotal += sum;
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			flush(context);
			super.cleanup(context);
		}

		private void flush(Context context) throws IOException,
				InterruptedException {
			Iterator<Entry<String, Integer>> it = mMap.entrySet().iterator();
			Map<String, Float> updatedPairs = new TreeMap<String, Float>();

			while (it.hasNext()) {
				Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>) it
						.next();
				updatedPairs.put(pairs.getKey(), (float) pairs.getValue()
						/ mTotal);
			}
			String output = getMapAsString(updatedPairs);
			context.write(new Text(mPrevLeftKey), new Text(output));
		}

		public static String getMapAsString(Map<String, Float> map) {
			if (map == null) {
				return "";
			}

			StringBuilder builder = new StringBuilder();
			String ret = "";

			Iterator<Entry<String, Float>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Float> pairs = (Map.Entry<String, Float>) it
						.next();
				builder.append(String.format("(%s,%s)", pairs.getKey(),
						pairs.getValue()));
				builder.append(" ");
			}

			// remove the last space if it exists
			if (builder.length() > 0) {
				ret = builder.substring(0, builder.length() - 1);
			}
			return ret;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new HybridApproach(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());

		job.setMapperClass(ParisStripesMapper.class);
		job.setReducerClass(ParisStripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(HybridApproach.class);
		job.waitForCompletion(true);

		job.submit();
		return 0;
	}

}
