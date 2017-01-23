package com.phantom.storm.stormproject.shop1.hbase.dao.imp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.phantom.storm.stormproject.shop1.hbase.dao.HBaseDAO;

public class HBaseDAOImp implements HBaseDAO {

	HConnection hTablePool = null;

	public HBaseDAOImp() {

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "slave");
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void save(Put put, String tableName) {

		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(put);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void insert(String tableName, String rowKey, String family, String quailifer, String value) {

		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes());
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void save(List<Put> Put, String tableName) {

		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(Put);
		} catch (Exception e) {

		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public Result getOneRow(String tableName, String rowKey) {

		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			rsResult = table.get(get);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKeyLike) {

		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) {

		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes());
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rs : scanner) {
				list.add(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String startRow, String stopRow) {

		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(stopRow.getBytes());
			ResultScanner scanner = table.getScanner(scan);
			list = new ArrayList<Result>();
			for (Result rsResult : scanner) {
				list.add(rsResult);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	public static void main(String[] args) {

		HBaseDAO dao = new HBaseDAOImp();
		// List<Put> list = new ArrayList<Put>();
		// Put put = new Put("cloudy".getBytes());
		// put.add("cf".getBytes(), "name".getBytes(), "zhaoliu1".getBytes()) ;
		// list.add(put) ;
		//// dao.save(put, "test") ;
		// put.add("cf".getBytes(), "addr".getBytes(), "shanghai1".getBytes()) ;
		// list.add(put) ;
		// put.add("cf".getBytes(), "age".getBytes(), "30".getBytes()) ;
		// list.add(put) ;
		// put.add("cf".getBytes(), "tel".getBytes(), "13567882341".getBytes())
		// ;
		// list.add(put) ;
		//
		// dao.save(list, "test");
		// dao.save(put, "test") ;
		// dao.insert("test", "testrow", "cf", "age", "35") ;
		// dao.insert("test", "testrow", "cf", "cardid", "12312312335") ;
		// dao.insert("test", "testrow", "cf", "tel", "13512312345") ;
		List<Result> list = dao.getRows("test", "rk", new String[] { "name", "age" });
		for (Result rs : list) {
			for (KeyValue keyValue : rs.raw()) {
				System.out.println("rowkey:" + new String(keyValue.getRow()));
				System.out.println("Qualifier:" + new String(keyValue.getQualifier()));
				System.out.println("Value:" + new String(keyValue.getValue()));
				System.out.println("----------------");
			}
		}
		// Result rs = dao.getOneRow("test", "testrow");
	}
}
