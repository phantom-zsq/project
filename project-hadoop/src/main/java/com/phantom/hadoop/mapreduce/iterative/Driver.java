package com.phantom.hadoop.mapreduce.iterative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCountMapper -> WordCountReducer -> WordCountMapper -> WordCountReducer
 * 迭代式编程
 * 
 * @author 张少奇
 * @time 2016年10月21日 下午3:32:21
 */
public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		int returnCode = ToolRunner.run(conf, new Driver(), args);
		System.exit(returnCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//任务1
		Configuration conf1 = new Configuration(this.getConf());
		Job job1 = Job.getInstance(conf1, "iterative");
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(WordCountMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		Path inPath = new Path(args[0]);
		Path outPath1 = new Path(args[1]);
		FileInputFormat.addInputPath(job1, inPath);
		FileOutputFormat.setOutputPath(job1, outPath1);
		
		boolean isSuccess = job1.waitForCompletion(true);
		if(!isSuccess){
			return 1;
		}
		
		//任务2
		Configuration conf2 = new Configuration(this.getConf());
		Job job2 = Job.getInstance(conf2, "iterative");
		job2.setJarByClass(Driver.class);

		job2.setMapperClass(WordCountMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setReducerClass(WordCountReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		Path outPath2 = new Path(args[2]);
		FileInputFormat.addInputPath(job2, outPath1);
		FileOutputFormat.setOutputPath(job2, outPath2);
		
		isSuccess = job2.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
}