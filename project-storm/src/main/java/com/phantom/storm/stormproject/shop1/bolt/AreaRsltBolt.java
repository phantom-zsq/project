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

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String date_areaid = input.getString(0);
		double order_amt = input.getDouble(1);
		countsMap.put(date_areaid, order_amt);
		// put into hbase
		// 2014-05-05_1,amt
		dao.insert("ns1:area_order", date_areaid, "cf", "order_amt", order_amt + "");
		System.err.println("rsltBolt put hbase: key=" + date_areaid + "; order_amt=" + order_amt);
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
