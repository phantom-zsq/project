package com.phantom.other.masterslave.slave.failure.slave;

import java.util.List;

import com.phantom.other.masterslave.master.MasterState;

/**
 * Slave端监控Master故障，并做响应的处理
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午3:06:20
 */
public interface MasterFailureHandler {

	void handle(List<MasterState> masterStateList);
}
