package com.phantom.hadoop.mapreduce.chain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Filter102ChainReducer extends Mapper<Text, Text, Text, Text>{
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(!"102".equals(key.toString())){
			context.write(null, value);
		}
	}
}