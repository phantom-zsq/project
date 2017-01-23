package com.phantom.storm.practice.trident.diagnosis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

public class OutbreakDetectionTopology {
	
	private static final String TOPOLOGY_NAME = "cdc";

	public static StormTopology buildTopology() {

		TridentTopology topology = new TridentTopology();
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		Stream inputStream = topology.newStream("event", spout);
		inputStream.each(new Fields("event"), new DiseaseFilter())
				.each(new Fields("event"), new CityAssignment(), new Fields("city"))
				.each(new Fields("event", "city"), new HourAssigment(), new Fields("hour", "cityDiseaseHour"))
				.groupBy(new Fields("cityDiseaseHour"))
				.persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count")).newValuesStream()
				.each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
				.each(new Fields("alert"), new DispatchAlert(), new Fields());
		return topology.build();
	}
	
	public static void main(String[] args) throws Exception {
		
		Config conf = new Config();
		conf.setNumWorkers(5);
		if(args.length==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf, buildTopology());
			Thread.sleep(200000);
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		}else{
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
}