package com.phantom.storm.practice.trident.diagnosis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class HourAssigment extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(HourAssigment.class);

	public void execute(TridentTuple tuple, TridentCollector collector) {

		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		String city = (String) tuple.getValue(1);
		long timestamp = diagnosis.time;
		long hourSinceEpoch = timestamp / 1000 / 60 / 60;
		LOG.debug("Key = [" + city + ":" + hourSinceEpoch + "]");
		String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;
		List<Object> values = new ArrayList<Object>();
		values.add(hourSinceEpoch);
		values.add(key);
		collector.emit(values);
	}
}