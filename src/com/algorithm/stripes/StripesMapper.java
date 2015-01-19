/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.stripes;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StripesMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] words = value.toString().split(" ");

		for (int i = 0; i < words.length; i++) {
			MapWritable stripe = new MapWritable();
			String termToProcess = words[i];

			for (int j = i + 1; j < words.length; j++) {
				if (termToProcess.equals(words[j]))
					break;
				if (stripe.containsKey(words[j])) {
					IntWritable textValue = (IntWritable) stripe.get(words[j]);
					stripe.put(new Text(words[j]),
							new IntWritable(textValue.get() + 1));
				} else {
					stripe.put(new Text(words[j]), new IntWritable(1));
				}
			}
			if (!stripe.isEmpty()) {
				context.write(new Text(termToProcess), stripe);
			}
		}
	}
}
