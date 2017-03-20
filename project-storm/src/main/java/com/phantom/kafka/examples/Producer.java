package com.phantom.kafka.examples;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread {

	private kafka.javaapi.producer.Producer<String, String> producer;
	private String topic = "topic_21";

	public Producer() {

		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "master:9092,slave:9092");
		producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
	}

	public Producer(String topic) {

		this();
		this.topic = topic;
	}

	public void run() {

		for (int i = 0; i < 1000; i++) {
			producer.send(new KeyedMessage<String, String>(topic, "1", new String(""+i)));
		}
	}
}
