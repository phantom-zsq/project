package com.phantom.kafka.stormkafka;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MyKafkaBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	public void cleanup() {

	}

	public void execute(Tuple input, BasicOutputCollector collector) {

		String kafkaMsg = input.getString(0);
		System.err.println("bolt:" + kafkaMsg);
	}

	public void prepare(Map stormConf, TopologyContext context) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {

		return null;
	}
}
