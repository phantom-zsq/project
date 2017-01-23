package com.phantom.storm.stormproject.shop1.bolt;

import java.util.Map;

import com.phantom.storm.stormproject.shop1.tools.DateFmt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AreaFilterBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// order_id,order_amt,create_time,area_id
		String order = input.getString(0);
		if (order != null) {
			String orderArr[] = order.split("\\t");
			// ared_id,order_amt,create_time
			collector.emit(new Values(orderArr[3], orderArr[1], DateFmt.getCountDate(orderArr[2], DateFmt.date_short)));
			System.err.println("AreaFilterBolt:"+order);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("area_id", "order_amt", "order_date"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
