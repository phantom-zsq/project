package com.phantom.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

/**
 * 大部分的类名同hbase操作名
 * @author 张少奇
 * @time 2016年10月19日 下午5:57:21
 */
public class HBaseClientApp {

	/**
	 * 主方法
	 * @author 张少奇
	 * @time 2016年10月19日 下午5:57:11 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// get操作
		 getData();

		// scan操作
		scanData();

		// delete操作
		 deleteData();

		// put操作
		 putData();

		// ddl操作
		ddl();

	}
	
	/**
	 * 获得配置文件Configuration类
	 * @author 张少奇
	 * @time 2016年10月19日 下午5:39:29 
	 * @return
	 */
	private static Configuration getConfiguration(){
		return HBaseConfiguration.create();
	}

	/**
	 * 根据表名获取HTable
	 * 
	 * @author 张少奇
	 * @time 2016年10月19日 下午4:31:02
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	private static HTable getHTableByTableName(String tableName) throws Exception {

		Configuration configuration = getConfiguration();
		HTable table = new HTable(configuration, tableName);
		return table;
	}
	
	/**
	 * put操作
	 * 
	 * @author 张少奇
	 * @time 2016年10月19日 下午4:39:44
	 * @throws Exception
	 */
	private static void putData() throws Exception {

		String tableName = "ns1:t1";
		HTable table = getHTableByTableName(tableName);

		Put put = new Put(Bytes.toBytes("20161019"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("40"));

		table.put(put);
		IOUtils.closeStream(table);
	}

	/**
	 * get操作
	 * 
	 * @author 张少奇
	 * @time 2016年10月19日 下午5:02:57
	 * @throws Exception
	 */
	private static void getData() throws Exception {

		String tableName = "ns1:t1";
		HTable table = getHTableByTableName(tableName);

		Get get = new Get(Bytes.toBytes("20160829"));
		get.addFamily(Bytes.toBytes("f1"));

		Result result = table.get(get);
		for (Cell cell : result.rawCells()) {
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println(cell.getTimestamp());
			System.out.println("--------------------------------");
		}
		IOUtils.closeStream(table);
	}

	/**
	 * delete操作
	 * 
	 * @author 张少奇
	 * @time 2016年10月19日 下午5:06:49
	 * @throws Exception
	 */
	private static void deleteData() throws Exception {

		String tableName = "ns1:t1";
		HTable table = getHTableByTableName(tableName);

		Delete delete = new Delete(Bytes.toBytes("20161019"));
		delete.deleteFamily(Bytes.toBytes("f1"));

		table.delete(delete);
		IOUtils.closeStream(table);
	}

	/**
	 * scan操作
	 * 
	 * @author 张少奇
	 * @time 2016年10月19日 下午5:22:35
	 * @throws Exception
	 */
	private static void scanData() throws Exception {

		String tableName = "ns1:t1";
		HTable table = getHTableByTableName(tableName);

		Scan scan = new Scan();

		// Range(包前不包尾)
		scan.setStartRow(Bytes.toBytes("20160829"));
		scan.setStopRow(Bytes.toBytes("20160831"));

		// Bytes.toBytes("")

		// add Column
		scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));

		// Filter
		Filter filter = new PrefixFilter(Bytes.toBytes("2016"));
		scan.setFilter(filter);

		// Cache
		scan.setCacheBlocks(true);
		scan.setCaching(2);
		scan.setBatch(2);

		ResultScanner resultScanner = table.getScanner(scan);

		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println(cell.getTimestamp());
				System.out.println("--------------------------------");
			}
		}

		IOUtils.closeStream(resultScanner);
		IOUtils.closeStream(table);
	}

	/**
	 * ddl操作
	 * @author 张少奇
	 * @time 2016年10月19日 下午6:04:21 
	 * @throws Exception
	 */
	private static void ddl() throws Exception{

		Configuration configuration = getConfiguration();
		HBaseAdmin admin = new HBaseAdmin(configuration);
		
		TableName tableName = TableName.valueOf(Bytes.toBytes("ns1:t1"));
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
		
		//表是否存在
		if(admin.tableExists(tableName)){
			if(admin.isTableAvailable(tableName)){
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
			
		}else{
			admin.createTable(tableDesc);
		}
		IOUtils.closeStream(admin);
	}
}