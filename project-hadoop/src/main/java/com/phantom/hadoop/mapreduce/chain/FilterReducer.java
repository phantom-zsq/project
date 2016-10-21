package com.phantom.hadoop.mapreduce.chain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text text : values){
			context.write(key, text);
		}
	}
}