package com.phantom.kafka.examples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer extends Thread {

	private ConsumerConnector consumer;
	private String topic = "topic1";

	public Consumer() {

		Properties props = new Properties();
		props.put("zookeeper.connect", "slave:2181");
		props.put("group.id", "test_group");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000000");
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	public Consumer(String topic) {

		this();
		this.topic = topic;
	}

	// push的方式
	public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(2));// 后面数字还应该是开几个线程去读取消息
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		ExecutorService service = Executors.newFixedThreadPool(4);
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			service.submit(new Runnable() {
				public void run() {
					ConsumerIterator<byte[], byte[]> it = stream.iterator();
					while (it.hasNext())
						System.out.println(new String(it.next().message()));
				}
			});
		}
	}
}
