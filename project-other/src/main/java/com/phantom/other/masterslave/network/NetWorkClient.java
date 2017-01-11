package com.phantom.other.masterslave.network;

import com.phantom.other.masterslave.command.Command;

/**
 * 网络客户端，运行在Slave节点
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午3:02:07
 */
public interface NetWorkClient {

	String NET_WORK_CLIENT_THREAD_NAME = "MS-NetWorkClient-Thread";

	String SLAVE_COMMAND_DISPATCHE_THREAD_NAME = "MS-Slave-Command-Dispatche-Thread";

	String SLAVE_HEART_BEAT_THREAD = "MS-Slave-Heartbeat-Thread";

	void init();

	void start();

	void stop();

	void send(Command command);
}
