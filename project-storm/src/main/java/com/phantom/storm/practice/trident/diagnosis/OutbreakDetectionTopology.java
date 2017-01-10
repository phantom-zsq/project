package com.phantom.storm.practice.trident.diagnosis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

public class OutbreakDetectionTopology {

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
		return null;
	}
	
	public static void main(String[] args) throws Exception {
		
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cdc", conf, buildTopology());
		Thread.sleep(200000);
		cluster.shutdown();
	}
}