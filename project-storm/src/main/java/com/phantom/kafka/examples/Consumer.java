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
import kafka.message.MessageAndMetadata;

public class Consumer extends Thread {

	private ConsumerConnector consumer;
	private String topic = "topic11";

	public Consumer() {

		Properties props = new Properties();
		props.put("zookeeper.connect", "slave:2181");
		props.put("group.id", "group11");
		props.put("auto.commit.interval.ms", "1000");
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
	}

	public Consumer(String topic) {

		this();
		this.topic = topic;
	}
	
	// push的方式
	public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		
		KafkaStream<byte[], byte[]> steam = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iterator = steam.iterator();
        while(iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println(message);
        }
        
//		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//		ExecutorService service = Executors.newFixedThreadPool(1);
//		for (final KafkaStream<byte[], byte[]> stream : streams) {
//			service.submit(new Runnable() {
//				public void run() {
//					ConsumerIterator<byte[], byte[]> it = stream.iterator();
//					while (it.hasNext()){
//						MessageAndMetadata<byte[], byte[]> value = it.next();
//						System.out.println(value.offset());
//						System.out.println(value.key());
//						System.out.println(value.message());
//					}
//				}
//			});
//		}
	}
}
