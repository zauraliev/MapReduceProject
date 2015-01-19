/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.pairs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PairsMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Map<String, Integer> map = new HashMap<String, Integer>();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");

		for (int i = 0; i < words.length; i++) {
			String term = words[i];
			for (int j = i + 1; j < words.length; j++) {
				if (term.equals(words[j]))
					break;

				String keyTerm = "(" + term + "," + words[j] + ")";
				String countTerm = "(" + term + ",*" + ")";

				if (!map.containsKey(keyTerm)) {
					map.put(keyTerm, 1);
				} else {
					map.put(keyTerm, map.get(keyTerm) + 1);
				}

				if (!map.containsKey(countTerm)) {
					map.put(countTerm, 1);
				} else {
					map.put(countTerm, map.get(countTerm) + 1);
				}
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		Iterator it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			context.write(new Text(pairs.getKey().toString()), new IntWritable(
					(Integer) pairs.getValue()));
			it.remove();
		}
	}
}
