package com.phantom.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从ns1:t1表中读取f1:name和f1:age的值放到ns1:t2表中
 * 
 * @author 张少奇
 * @time 2016年10月20日 下午3:21:30
 */
public class ExportBasicFromUserMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		// create conf
		Configuration configuration = HBaseConfiguration.create();

		// run job
		int status = ToolRunner.run(configuration, new ExportBasicFromUserMapReduce(), args);

		// exit program
		System.exit(status);
	}

	public int run(String[] args) throws Exception {

		// 1) get conf
		Configuration conf = super.getConf();

		// 2) create job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		// 3) set job
		job.setJarByClass(ExportBasicFromUserMapReduce.class);

		// input & mapper
		Scan scan = new Scan();
		scan.setCaching(500);//默认为1个
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		scan.setBatch(10);

		TableMapReduceUtil.initTableMapperJob("ns1:t1", // input table
				scan, // Scan instance to control CF and attribute selection
				ReadFromUserMapper.class, // mapper class
				ImmutableBytesWritable.class, // mapper output key
				Put.class, // mapper output value
				job);

		// reducer & output
		TableMapReduceUtil.initTableReducerJob("ns1:t2", // output table
				WriteToBasicReducer.class, // reducer class
				job);

		job.setNumReduceTasks(1); // at least one, adjust as required

		// 4) submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	public static class ReadFromUserMapper extends TableMapper<ImmutableBytesWritable, Put> {

		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			// create put
			Put put = new Put(key.get());

			// iterator
			for (Cell cell : value.rawCells()) {
				// add family: info
				if ("f1".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
					// add column : name and age
					if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						put.add(cell);
					} else if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						put.add(cell);
					}
				}
			}
			// output
			context.write(key, put);
		}
	}

	public static class WriteToBasicReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
				throws IOException, InterruptedException {
			for (Put put : values) {
				context.write(key, put);
			}
		}
	}
}