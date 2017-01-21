package com.phantom.other.kafka.examples;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread {

	private kafka.javaapi.producer.Producer<Integer, String> producer;
	private String topic = "topic1";

	public Producer() {

		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "slave:9092");
		producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
	}
	
	public Producer(String topic) {

		this();
		this.topic = topic;
	}

	public void run() {

		for (int i = 0; i < 10; i++) {
			producer.send(new KeyedMessage<Integer, String>(topic, new String("Message_" + i)));
		}
	}
}
