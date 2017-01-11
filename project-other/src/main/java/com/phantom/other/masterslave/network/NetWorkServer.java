package com.phantom.other.masterslave.network;

/**
 * 网络服务器，运行在Master节点
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午2:51:34
 */
public interface NetWorkServer {

	String NET_WORK_SERVER_THREAD_NAME = "MS-NetWorkServer-Thread";

	void init();

	void start();

	void stop();

	String getIp();

	int getPort();
}
