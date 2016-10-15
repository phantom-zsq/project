package com.phantom.hadoop.hadoopproject.lvmama.mr.etls;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AnalyserLogDataMapper extends Mapper<Object, Text, NullWritable, Put> {
	
	private static final Logger logger = Logger.getLogger(AnalyserLogDataMapper.class);
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		logger.debug("value:" + value);
		
		//拆分value
		Map<String,String> map = chair(value);
		
		//解析失败
		if(map==null){
			return;
		}
		
		//过滤无效数据
		if(!map.containsKey("key")){
			return;
		}
		
		//生成新的数据
		byte[] rowkey = Bytes.toBytes("rowkey");
		Put put = new Put(rowkey);
		put.add(Bytes.toBytes("family"),Bytes.toBytes("column"),Bytes.toBytes("value"));
		
		context.write(NullWritable.get(), put);
	}
	
	private Map<String,String> chair(Text value){
		
		Map<String,String> map = new HashMap<String,String>();
		map.put("key", value.toString());
		return map;
	}
}