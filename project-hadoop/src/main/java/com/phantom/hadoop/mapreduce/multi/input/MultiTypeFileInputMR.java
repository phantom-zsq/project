package com.phantom.hadoop.mapreduce.multi.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 不同数据源执行不同的map操作
 * 
 * @author 张少奇
 * @time 2016年10月21日 下午6:27:46
 */
public class MultiTypeFileInputMR extends Configured implements Tool {

	public static class MultiTypeFileInput1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] str = value.toString().split("\\|");
			context.write(new Text(str[0]), new Text(str[1]));
		}
	}

	public static class MultiTypeFileInput2Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] str = value.toString().split(" ");
			context.write(new Text(str[0]), new Text(str[1]));
		}
	}

	public static class MultiTypeFileInputReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int returnCode = ToolRunner.run(conf, new MultiTypeFileInputMR(), args);
		System.exit(returnCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		// 输出文本以逗号分隔
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "MultiPathFileInput");
		job.setJarByClass(this.getClass());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(MultiTypeFileInputReducer.class);
		job.setNumReduceTasks(1);

		Path inPath1 = new Path(args[0]);
		Path inPath2 = new Path(args[0]);
		Path outPath = new Path(args[1]);

		MultipleInputs.addInputPath(job, inPath1, TextInputFormat.class, MultiTypeFileInput1Mapper.class);
		MultipleInputs.addInputPath(job, inPath2, TextInputFormat.class, MultiTypeFileInput2Mapper.class);

		FileOutputFormat.setOutputPath(job, outPath);

		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
}