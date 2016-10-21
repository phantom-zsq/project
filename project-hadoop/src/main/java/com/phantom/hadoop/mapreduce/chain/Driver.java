package com.phantom.hadoop.mapreduce.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * SplitMapper -> Filter101ChainMapper -> FilterReducer -> Filter102ChainMapper
 * 链式编程
 * 
 * @author 张少奇
 * @time 2016年10月21日 下午3:16:26
 */
public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int returnCode = ToolRunner.run(conf, new Driver(), args);
		System.exit(returnCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "chain");
		job.setJarByClass(Driver.class);

		Configuration splitConf = new Configuration(false);
		ChainMapper.addMapper(job, SplitMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
				splitConf);

		Configuration filter101Conf = new Configuration(false);
		ChainMapper.addMapper(job, Filter101ChainMapper.class, Text.class, Text.class, Text.class, Text.class,
				filter101Conf);

		Configuration reducerConf = new Configuration(false);
		ChainReducer.setReducer(job, FilterReducer.class, Text.class, Text.class, Text.class, Text.class, reducerConf);

		Configuration filter102Conf = new Configuration(false);
		ChainReducer.addMapper(job, Filter102ChainReducer.class, Text.class, Text.class, Text.class, Text.class,
				filter102Conf);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
}