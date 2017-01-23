package com.phantom.storm.stormproject.shop1.kafka.consumers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class OrderConsumer extends Thread {

	private ConsumerConnector consumer;
	private String topic = "topic1";
	private Queue<String> queue = new ConcurrentLinkedQueue<String>();
	
	public OrderConsumer() {

		Properties props = new Properties();
		props.put("zookeeper.connect", "slave:2181");
		props.put("group.id", "topic1_group");
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	public OrderConsumer(String topic) {

		this();
		this.topic = topic;
	}

	// push消费方式，服务端推送过来。主动方式是pull
	public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			// 逻辑处理
			queue.add(new String(it.next().message()));
		}
	}

	public Queue<String> getQueue() {
		return queue;
	}

	public static void main(String[] args) {
		
		OrderConsumer consumerThread = new OrderConsumer();
		consumerThread.start();
	}
}
