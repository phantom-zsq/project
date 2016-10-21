package com.phantom.hadoop.mapreduce.dependent;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Text mapOutputKey = new Text();
	private IntWritable mapOutputValue = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String word = null;
		StringTokenizer st = new StringTokenizer(value.toString());
		while (st.hasMoreTokens()) {
			word = st.nextToken();
			mapOutputKey.set(word);
			context.write(mapOutputKey, mapOutputValue);
		}
	}
}
