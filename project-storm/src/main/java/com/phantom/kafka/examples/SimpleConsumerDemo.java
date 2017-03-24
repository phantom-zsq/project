package com.phantom.kafka.examples;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.HashMap;
import java.util.Map;

public class SimpleConsumerDemo {

	private static String topic2 = "topic2";
	private static String topic3 = "topic3";
	private static String clientId = "SimpleConsumerDemoClient";

	private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {

		for (MessageAndOffset messageAndOffset : messageSet) {
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			System.out.println(new String(bytes, "UTF-8"));
		}
	}

	public static void main(String[] args) throws Exception {

		SimpleConsumer simpleConsumer = new SimpleConsumer("slave", 9092, 100000, 64 * 1024, clientId);
		System.out.println("Testing single fetch");
		FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic2, 0, 0L, 100).build();
		FetchResponse fetchResponse = simpleConsumer.fetch(req);
		printMessages(fetchResponse.messageSet(topic2, 0));
		System.out.println("Testing single multi-fetch");
		Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>() {
			{
				put(topic2, new ArrayList<Integer>() {
					{
						add(0);
					}
				});
				put(topic3, new ArrayList<Integer>() {
					{
						add(0);
					}
				});
			}
		};
		req = new FetchRequestBuilder().clientId(clientId).addFetch(topic2, 0, 0L, 100).addFetch(topic3, 0, 0L, 100).build();
		fetchResponse = simpleConsumer.fetch(req);
		int fetchReq = 0;
		for (Map.Entry<String, List<Integer>> entry : topicMap.entrySet()) {
			String topic = entry.getKey();
			for (Integer offset : entry.getValue()) {
				System.out.println("Response from fetch request no: " + ++fetchReq);
				printMessages(fetchResponse.messageSet(topic, offset));
			}
		}
	}
}
