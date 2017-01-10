package com.phantom.storm.practice.trident.diagnosis;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

public class DiagnosisEventSpout implements ITridentSpout<Long> {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector;
	BatchCoordinator<Long> coordinator = new DefaultCoordinator();
	Emitter<Long> emitter = new DiagnosisEventEmitter();

	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return coordinator;
	}

	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return emitter;
	}

	public Map getComponentConfiguration() {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("event");
	}
}