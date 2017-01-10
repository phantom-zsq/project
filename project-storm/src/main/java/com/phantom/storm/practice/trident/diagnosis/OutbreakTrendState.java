package com.phantom.storm.practice.trident.diagnosis;

import storm.trident.state.map.NonTransactionalMap;

public class OutbreakTrendState extends NonTransactionalMap<Long>{

	protected OutbreakTrendState(OutbreakTrendBackingMap outbreakTrendBackingMap) {
		super(outbreakTrendBackingMap);
	}
}