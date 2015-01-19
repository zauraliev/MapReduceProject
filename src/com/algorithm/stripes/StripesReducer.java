/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.stripes;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (MapWritable listStripePair : values) {
			for(Entry<Writable, Writable> pair : listStripePair.entrySet()){
				if(map.containsKey(pair.getKey())){
					map.put(pair.getKey().toString(), ((IntWritable)pair.getValue()).get() + map.get(pair.getKey().toString()));
				}
				else
				{
					map.put(pair.getKey().toString(), ((IntWritable)pair.getValue()).get());
				}
			}
		}

		int totalCount = 0;
		for (Map.Entry entry : map.entrySet()) {
			totalCount += (Integer)entry.getValue();
		}
		
		Double doubleTotalCount = (double) totalCount;

		StringBuilder stringBuilder = new StringBuilder();

		for (Map.Entry entry : map.entrySet()) {
			stringBuilder.append(entry.getKey()).append(" : ")
					.append((Integer)entry.getValue() / doubleTotalCount)
					.append(" ; ");
		}
		context.write(key, new Text("(" + stringBuilder.toString() + ")"));
	}
}