package com.phantom.kafka.examples;

public class KafkaConsumerProducerDemo {

	public static void main(String[] args) {

		Producer producerThread = new Producer();
		producerThread.start();

		Consumer consumerThread = new Consumer();
		consumerThread.start();
	}
}
