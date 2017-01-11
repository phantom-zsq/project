package com.phantom.other.masterslave;

/**
 * 分布式节点，Master节点MasterNode和Slave节点SlaveNode实现本接口
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午2:48:12
 */
public interface Node extends Runnable {

	/**
	 * 节点初始化
	 */
	void init();

	/**
	 * 节点启动
	 */
	void start();

	/**
	 * 节点停止
	 */
	void stop();

	/**
	 * 节点重启
	 */
	void reset();

	/**
	 * 收集统计信息
	 */
	State gatherStatistics();

	/**
	 * 接受统计信息
	 */
	void acceptStatistics(State state);
}
