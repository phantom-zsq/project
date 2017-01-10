package com.phantom.storm.practice.trident.diagnosis;

import jline.internal.Log;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DispatchAlert extends BaseFunction{

	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		String alert = (String) tuple.getValue(0);
		Log.error("ALERT DECEIVED [" + alert + "]");
		Log.error("Dispatch the national guard!");
		System.exit(0);
	}
}