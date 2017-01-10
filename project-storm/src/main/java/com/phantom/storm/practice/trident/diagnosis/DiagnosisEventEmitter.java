package com.phantom.storm.practice.trident.diagnosis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

public class DiagnosisEventEmitter implements Emitter<Long>, Serializable {

	private static final long serialVersionUID = 1L;
	AtomicInteger successfulTransactions = new AtomicInteger(0);

	public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {

		for (int i = 0; i < 10000; i++) {

			List<Object> events = new ArrayList<Object>();
			double lat = new Double(-30 + (int) (Math.random() * 75));
			double lng = new Double(-120 + (int) (Math.random() * 70));
			long time = System.currentTimeMillis();
			String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
			DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
			events.add(event);
			collector.emit(events);
		}
	}

	public void success(TransactionAttempt tx) {

		successfulTransactions.incrementAndGet();
	}

	public void close() {

	}
}