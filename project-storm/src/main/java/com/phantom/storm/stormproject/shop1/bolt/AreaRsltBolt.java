package com.phantom.storm.stormproject.shop1.bolt;

import java.util.HashMap;
import java.util.Map;

import com.phantom.storm.stormproject.shop1.hbase.dao.HBaseDAO;
import com.phantom.storm.stormproject.shop1.hbase.dao.imp.HBaseDAOImp;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AreaRsltBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	Map<String, Double> countsMap = null;
	HBaseDAO dao = null;
	long beginTime = System.currentTimeMillis();
	long endTime = 0L;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String date_areaid = input.getString(0);
		double order_amt = input.getDouble(1);
		countsMap.put(date_areaid, order_amt);

		endTime = System.currentTimeMillis();
		if (endTime - beginTime >= 5 * 1000) {
			for (String key : countsMap.keySet()) {
				// put into hbase
				// 2014-05-05_1,amt
				dao.insert("area_order", key, "cf", "order_amt", countsMap.get(key) + "");
				System.err.println("rsltBolt put hbase: key=" + key + "; order_amt=" + countsMap.get(key));
			}
			beginTime = System.currentTimeMillis();
		}

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		dao = new HBaseDAOImp();
		countsMap = new HashMap<String, Double>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
