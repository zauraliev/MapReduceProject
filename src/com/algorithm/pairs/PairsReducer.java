/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.pairs;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class PairsReducer extends Reducer<Text, IntWritable, Text, Text> {
	
	int totalCount = 0;

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		if (key.toString().contains("*")) {
			totalCount = sum;
		} else {
			double value = (double) sum / (double) totalCount;
			context.write(key, new Text(value + ""));
		}
	}
}
