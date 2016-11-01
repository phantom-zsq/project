package com.phantom.hadoop.mapreduce.dependent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCountMapper -> WordCountReducer
 * 									 -> WordCountMapper -> WordCountReducer
 * WordCountMapper -> WordCountReducer 
 * 依赖关系组合式编程
 * 
 * @author 张少奇
 * @time 2016年10月21日 下午3:32:21
 */
public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int returnCode = ToolRunner.run(conf, new Driver(), args);
		System.exit(returnCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		// 任务1
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

		// 任务2
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
		FileInputFormat.addInputPath(job2, inPath);
		FileOutputFormat.setOutputPath(job2, outPath2);

		// 任务3
		Configuration conf3 = new Configuration(this.getConf());
		Job job3 = Job.getInstance(conf3, "iterative");
		job3.setJarByClass(Driver.class);

		job3.setMapperClass(WordCountMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);

		job3.setReducerClass(WordCountReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		Path outPath3 = new Path(args[3]);
		FileInputFormat.addInputPath(job3, outPath1);
		FileInputFormat.addInputPath(job3, outPath2);
		FileOutputFormat.setOutputPath(job3, outPath3);
		
		// 控制
		ControlledJob cjob1 = new ControlledJob(conf1);
		cjob1.setJob(job1);
		ControlledJob cjob2 = new ControlledJob(conf2);
		cjob2.setJob(job2);
		ControlledJob cjob3 = new ControlledJob(conf3);
		cjob3.setJob(job3);

		cjob3.addDependingJob(cjob1);// 设置job3和job1的依赖关系
		cjob3.addDependingJob(cjob2);

		JobControl jobCtrl = new JobControl("123");
		jobCtrl.addJob(cjob1);
		jobCtrl.addJob(cjob2);
		jobCtrl.addJob(cjob3);

		Thread t = new Thread(jobCtrl);
		t.start();

		while (true) {

			if (jobCtrl.allFinished()) {// 如果作业成功完成，就打印成功作业的信息
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();
				break;
			}

			if (jobCtrl.getFailedJobList().size() > 0) {// 如果作业失败，就打印失败作业的信息
				System.out.println(jobCtrl.getFailedJobList());
				jobCtrl.stop();
				break;
			}
		}
		return 0;
	}
}