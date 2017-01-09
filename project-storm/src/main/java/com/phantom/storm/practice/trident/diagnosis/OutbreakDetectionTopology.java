package com.phantom.storm.practice.trident.diagnosis;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class OutbreakDetectionTopology {

	public static StormTopology buildTopology(){
		
		TridentTopology topology = new TridentTopology();
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		Stream inputStream = topology.newStream("event", spout);
//		inputStream.each(new Fields("event"), new DiseaseFilter());
		return null;
	}
}