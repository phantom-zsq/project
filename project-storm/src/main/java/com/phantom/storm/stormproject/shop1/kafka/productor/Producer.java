package com.phantom.storm.stormproject.shop1.kafka.productor;

import java.util.Properties;
import java.util.Random;

import com.phantom.storm.stormproject.shop1.tools.DateFmt;

import backtype.storm.utils.Utils;
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

		// order_id,order_amt,create_time,area_id
		Random random = new Random();
		String[] order_amt = { "10.10", "20.10", "50.2", "60.0", "80.1" };
		String[] area_id = { "1", "2", "3", "4", "5" };

		int i = 0;
		while (true) {
			String messageStr = ++i + "\t" + order_amt[random.nextInt(5)] + "\t"
					+ DateFmt.getCountDate(null, DateFmt.date_long) + "\t" + area_id[random.nextInt(5)];
			System.err.println("product:" + messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			Utils.sleep(1000);
			if(i == 10){
				break;
			}
		}
		Utils.sleep(10000);
	}

	public static void main(String[] args) {

		Producer producerThread = new Producer();
		producerThread.start();
	}
}
