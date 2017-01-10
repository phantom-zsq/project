package com.phantom.storm.practice.trident.diagnosis;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

	public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {

		LOG.info("Initializing Transaction [" + txid + "]");
		return null;
	}

	public void success(long txid) {

		LOG.info("Successful Transaction [" + txid + "]");
	}

	public boolean isReady(long txid) {

		return true;
	}

	public void close() {

	}
}