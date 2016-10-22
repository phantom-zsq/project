package com.phantom.hadoop.rpc.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.phantom.hadoop.hadoopproject.lvmama.example.IHello;

/**
 * 启动服务器
 * 
 * @author phantom
 *
 */
public class HelloServer {

	public static void main(String[] args) throws Exception {

		// 创建一个上下文关系
		Configuration conf = new Configuration();
		// 创建一个对象
		IHello hello = new HelloServerImpl();
		// 创建服务
		Server server = new RPC.Builder(conf) // 传入上下文
				.setInstance(hello) // 指定服务器服务的对象
				.setProtocol(IHello.class) // 接口
				.setBindAddress("127.0.0.1") // 监听ip地址， 如果不给定，默认为当前ip地址
				.setPort(9999) // 监听端口号， 如果不给定，默认随机
				.setNumHandlers(5) // 进程数，默认为1个
				.setVerbose(true).build(); // 创建
		// 启动
		server.start();
		
		// 停止， 也可以直接kill进程
		// server.stop();
	}
}