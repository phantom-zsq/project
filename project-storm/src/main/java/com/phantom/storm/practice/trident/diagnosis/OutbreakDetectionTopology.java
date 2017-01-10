package com.phantom.storm.practice.trident.diagnosis;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class OutbreakDetectionTopology {

	public static StormTopology buildTopology() {

		TridentTopology topology = new TridentTopology();
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		Stream inputStream = topology.newStream("event", spout);
//		inputStream
//				.each(new Fields("event"), new DiseaseFilter())
//				.each(new Fields("event"), new CityAssignment(),
//						new Fields("city"))
//				.each(new Fields("event", "city"), new HourAssigment(),
//						new Fields("hour", "cityDiseaseHour"))
//				.groupBy(new Fields("cityDiseaseHour"))
//				.persistentAggregate(new OutbreakTrendFactory(), new Count(),
//						new Fields("count"))
//				.newValuesStream()
//				.each(new Fields("cityDiseaseHour", "count"),
//						new outbreakDetector(), new Fields("alert"))
//				.each(new Fields("alert"), new DispatchAlert(), new Fields());
		return null;
	}
}