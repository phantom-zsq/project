package com.phantom.storm.stormproject.shop1.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.phantom.storm.stormproject.shop1.hbase.dao.HBaseDAO;
import com.phantom.storm.stormproject.shop1.hbase.dao.imp.HBaseDAOImp;
import com.phantom.storm.stormproject.shop1.tools.DateFmt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AreaAmtBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	String today = null;
	HBaseDAO dao = null;
	Map<String, Double> countsMap = null;

	@Override
	public void cleanup() {

		countsMap.clear();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// area_id,count
		if (input != null) {
			String area_id = input.getString(0);
			double order_amt = 0.0;
			try {
				order_amt = Double.parseDouble(input.getString(1));
			} catch (Exception e) {
				System.out.println(input.getString(1) + ":---------------------------------");
				e.printStackTrace();
			}

			String order_date = input.getStringByField("order_date");
			if (!order_date.equals(today)) {
				// 跨天处理
				countsMap.clear();
			}

			Double count = countsMap.get(order_date + "_" + area_id);
			if (count == null) {
				count = 0.0;
			}
			count += order_amt;
			countsMap.put(order_date + "_" + area_id, count);
			System.err.println("areaAmtBolt:" + order_date + "_" + area_id + "=" + count);
			collector.emit(new Values(order_date + "_" + area_id, count));
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		dao = new HBaseDAOImp();
		// 根据HBase里初始值进行初始化 countsMap
		today = DateFmt.getCountDate(null, DateFmt.date_short);
		countsMap = this.initMap(today, dao);
//		for (String key : countsMap.keySet()) {
//			System.err.println("key:" + key + "; value:" + countsMap.get(key));
//		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("date_area", "amt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public Map<String, Double> initMap(String rowKeyDate, HBaseDAO dao) {
		
		Map<String, Double> countsMap = new HashMap<String, Double>();
		List<Result> list = dao.getRows("ns1:area_order", rowKeyDate, new String[] { "order_amt" });

		for (Result rsResult : list) {
			String rowKey = new String(rsResult.getRow());
			for (KeyValue keyValue : rsResult.raw()) {
				if ("order_amt".equals(new String(keyValue.getQualifier()))) {
					countsMap.put(rowKey, Double.parseDouble(new String(keyValue.getValue())));
					break;
				}
			}
		}
		return countsMap;
	}

}
