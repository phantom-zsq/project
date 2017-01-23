package com.phantom.storm.stormproject.shop1.spout;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.phantom.storm.stormproject.shop1.kafka.consumers.OrderConsumer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OrderBaseSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	
	Integer TaskId = null;
	SpoutOutputCollector collector = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("order"));
	}

	@Override
	public void nextTuple() {

		if (queue.size() > 0) {
			String str = queue.poll();
			collector.emit(new Values(str));
			System.err.println("OrderBaseSpout:"+str);
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		TaskId = context.getThisTaskId();
		OrderConsumer consumer = new OrderConsumer();
		consumer.start();
		queue = consumer.getQueue();
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void activate() {

	}

	@Override
	public void close() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
