package com.phantom.storm.practice.trident.diagnosis;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CityAssignment extends BaseFunction{

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);
	
	private static Map<String, double[]> CITIES = new HashMap<String, double[]>();
	
	{
		double[] phl = {39.875365, -75.249524};
		CITIES.put("PHL", phl);
		double[] nyc = {40.71448, -74.00568};
		CITIES.put("NYC", nyc);
		double[] sf = {-31.4250142, -62.0841809};
		CITIES.put("SF", sf);
		double[] la = {-34.05374, -118.24307};
		CITIES.put("LA", la);
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		double leastDistance = Double.MAX_VALUE;
		String closestCity = "NONE";
		
		for(Entry<String, double[]> city : CITIES.entrySet()){
			double R = 6371;
			double x = (city.getValue()[0] - diagnosis.lng) * Math.cos((city.getValue()[0] 
		}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
}