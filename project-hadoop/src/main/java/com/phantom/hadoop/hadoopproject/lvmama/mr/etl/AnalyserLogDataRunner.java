package com.phantom.hadoop.hadoopproject.lvmama.mr.etl;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.util.TimeUtil;

/**
 * etl程序入口执行类
 * 
 * @author 张少奇
 * @time 2016年10月15日 下午4:48:05
 */
public class AnalyserLogDataRunner implements Tool {

	private Configuration conf = null;

	public static void main(final String[] args) {

		// 以指定用户提交
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		// 如果dfs.permissions.enabled设置为true（默认为true）而且运行的程序或命令的用户名和hdfs的有权限的用户不是同一个，可使用UserGroupInformation来创建远程用户
		UserGroupInformation.createRemoteUser("hadoop").doAs(new PrivilegedAction<Object>() {

			@Override
			public Object run() {
				
				try {
					int exitCode = ToolRunner.run(new AnalyserLogDataRunner(), args);
					if (exitCode == 0) {
						System.out.println("运行成功");
					} else {
						System.out.println("运行失败");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}

	@Override
	public void setConf(Configuration that) {
		
		this.conf = HBaseConfiguration.create(that);
		// 本地提交集群运行, 需要添加的参数
		// this.conf.set("fs.defaultFS", "hdfs://master:8020");
		// this.conf.set("mapreduce.framework.name", "yarn");
		// this.conf.set("yarn.resourcemanager.address", "master:8032");
		// this.conf.set("mapreduce.app-submission.cross-platform", "true");
	}

	@Override
	public Configuration getConf() {
		
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		// 处理参数
		this.processArgs(conf, args);

		// 开始创建job
		Job job = Job.getInstance(conf, "analyser_data");
		// 设置job的相关信息, 一定不能少
		job.setJarByClass(AnalyserLogDataRunner.class);
		// 设置mapper
		job.setMapperClass(AnalyserLogDataMapper.class);
		// 输出key/value
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		// 设置reducer的个数为0，默认是1个
		job.setNumReduceTasks(0);

		// 设置输出到hbase的设置
		this.setHBaseOutputConfig(job);

		// 设置输入文件的位置信息
		this.setJobInputPaths(job);

		// 添加需要提交到集群上的jar文件
		// ((org.apache.hadoop.mapred.JobConf)job.getConfiguration()).setJar("target/transformer-0.0.1.jar");

		// 进行运行了
		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * 处理参数
	 * 
	 * @param conf
	 * @param args
	 */
	private void processArgs(Configuration conf, String[] args) {
		
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-d".equals(args[i])) {
				if (i + 1 < args.length) {
					date = args[++i];
					break;
				}
			}
		}

		// 需要默认参数
		if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
			// 默认给定昨天
			date = TimeUtil.getYesterday();
		}
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

	/**
	 * 设置job的输出日志路径
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setJobInputPaths(Job job) throws IOException {
		
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");
		if (GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/")) {
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + hdfsPath;
		} else {
			hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + "/" + hdfsPath;
		}

		Path inputPath = new Path(hdfsPath);
		if (fs.exists(inputPath)) {
			FileInputFormat.addInputPath(job, inputPath);
		} else {
			throw new RuntimeException("日志文件不存在，路径为：" + hdfsPath);
		}
		// 默认情况下：FileSystem会自动关闭，在jvm退出的时候，最好不要关闭
	}

	/**
	 * 设置hbase 输出
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setHBaseOutputConfig(Job job) throws IOException {
		
		// hbase表后缀
		String tableNameSuffix = TimeUtil.parseLong2String(
				TimeUtil.parseString2Long(job.getConfiguration().get(GlobalConstants.RUNNING_DATE_PARAMES)),
				TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);

		// 本地运行， window平台运行，某些环境中，需要将addDependency这个参数设置为false，默认为true
		TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix, null, job,
				null, null, null, null, false);
		// 集群运行/本地提交远程集群运行，addDependency参数必须设置为true
		TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix, null, job);

		// 如果表不存在，创建
		Configuration conf = job.getConfiguration();
		boolean override = conf.getBoolean(GlobalConstants.RUNNING_OVERRIDE_ETL_HBASE_TABLE, true);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			throw new RuntimeException("创建HBaseAdmin发生异常", e);
		}

		try {
			TableName tableName = TableName.valueOf(EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix);
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 设置family
			tableDesc.addFamily(new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME));

			if (override && admin.tableExists(tableName)) {
				// 表存在，而且覆盖
				if (admin.isTableEnabled(tableName)) {
					// 表如果处于可操作状态，设置为不可以操作状态
					admin.disableTable(tableName);
				}
				// 删除表
				admin.deleteTable(tableName);
			}

			// 创建表
			if (!admin.tableExists(tableName)) {
				// 不存在，那么创建
				admin.createTable(tableDesc);
			}
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (Exception e) {
					// nothings
				}
			}
		}
	}
}