package com.phantom.other.masterslave.slave.failure.slave;

import com.phantom.other.masterslave.master.MasterState;
import com.phantom.other.masterslave.network.NetWorkConstants;

/**
 * Slave端监控Master故障，并做响应的处理
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午3:06:30
 */
public interface MasterFailureMonitor {

	int DEFAULT_STORE_MASTER_STATE_SIZE = 10;
	long DEFAULT_FAILURE_MONITOR_INTERVAL = NetWorkConstants.DEFAULT_MS_HEART_BEAT_INTERVAL * 4;
	long DEFAULT_FAILURE_MONITOR_WAIT_MASTER_STATE_TIME_OUT = NetWorkConstants.DEFAULT_MS_HEART_BEAT_INTERVAL * 10;

	void init();

	void start();

	void stop();

	void process(MasterState masterState);
}
