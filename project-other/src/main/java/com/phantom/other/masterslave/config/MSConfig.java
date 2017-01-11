package com.phantom.other.masterslave.config;

/**
 * 系统配置
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午3:00:48
 */
public class MSConfig {

	public Long DEFAULT_SLAVE_COMMAND_PRODUCE_INTERVAL = 60L; // unit second

	private Long slaveCommandProduceInterval = DEFAULT_SLAVE_COMMAND_PRODUCE_INTERVAL;

	public Long getSlaveCommandProduceInterval() {
		return slaveCommandProduceInterval;
	}

	public void setSlaveCommandProduceInterval(Long slaveCommandProduceInterval) {
		this.slaveCommandProduceInterval = slaveCommandProduceInterval;
	}
}
