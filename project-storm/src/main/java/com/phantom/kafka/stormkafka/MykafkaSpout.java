package com.phantom.kafka.stormkafka;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class MykafkaSpout {

	public static void main(String[] args) {

		ZkHosts zkHosts = new ZkHosts("slave:2181");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts,
				"topic1",
				"/MyKafka", // 偏移量offset的根目录
				"MyTrack"); // 对应一个应用
		List<String> zkServers = new ArrayList<String>();
		for (String host : zkHosts.brokerZkStr.split(",")) {
			zkServers.add(host.split(":")[0]);
		}
		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = false; // 从头开始消费
		spoutConfig.socketTimeoutMs = 60 * 1000;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); // 定义输出为String

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("bolt", new MyKafkaBolt(), 1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
	}
}
