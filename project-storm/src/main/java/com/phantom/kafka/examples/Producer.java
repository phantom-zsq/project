package com.phantom.kafka.examples;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread {

	private kafka.javaapi.producer.Producer<byte[], byte[]> producer;
	private String topic = "topic11";

	public Producer() {

		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("metadata.broker.list", "master:9092,slave:9092");
		producer = new kafka.javaapi.producer.Producer<byte[], byte[]>(new ProducerConfig(props));
	}

	public Producer(String topic) {

		this();
		this.topic = topic;
	}

	public void run() {

		for (int i = 9; i < 100; i++) {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, (""+i).getBytes()));
		}
	}
}
