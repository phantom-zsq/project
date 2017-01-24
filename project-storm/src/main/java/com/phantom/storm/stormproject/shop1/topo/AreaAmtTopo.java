package com.phantom.storm.stormproject.shop1.topo;

import com.phantom.storm.stormproject.shop1.bolt.AreaAmtBolt;
import com.phantom.storm.stormproject.shop1.bolt.AreaFilterBolt;
import com.phantom.storm.stormproject.shop1.bolt.AreaRsltBolt;
import com.phantom.storm.stormproject.shop1.spout.OrderBaseSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AreaAmtTopo {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new OrderBaseSpout());
		builder.setBolt("filter", new AreaFilterBolt(), 5).shuffleGrouping("spout");
		builder.setBolt("areabolt", new AreaAmtBolt(), 2).fieldsGrouping("filter", new Fields("area_id"));
		builder.setBolt("rsltBolt", new AreaRsltBolt(), 1).shuffleGrouping("areabolt");
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);

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
