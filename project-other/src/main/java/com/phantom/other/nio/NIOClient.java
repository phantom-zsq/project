package com.phantom.other.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * NIO客户端
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午5:50:54
 */
public class NIOClient {

	// 通道管理器
	private Selector selector;

	/**
	 * 获得一个Socket通道，并对该通道做一些初始化的工作
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午5:51:18
	 * @param ip
	 * @param port
	 * @throws IOException
	 */
	public void initClient(String ip, int port) throws IOException {

		// 获得一个Socket通道
		SocketChannel channel = SocketChannel.open();
		// 设置通道为非阻塞
		channel.configureBlocking(false);
		// 获得一个通道管理器
		this.selector = Selector.open();
		// 客户端连接服务器,其实方法执行并没有实现连接，需要在listen()方法中调用channel.finishConnect();才能完成连接
		channel.connect(new InetSocketAddress(ip, port));
		// 将通道管理器和该通道绑定，并为该通道注册SelectionKey.OP_CONNECT事件。
		channel.register(selector, SelectionKey.OP_CONNECT);
	}

	/**
	 * 采用轮询的方式监听selector上是否有需要处理的事件，如果有，则进行处理
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午5:51:29
	 * @throws IOException
	 */
	public void listen() throws IOException {

		// 轮询访问selector
		while (true) {
			selector.select();
			// 获得selector中选中的项的迭代器
			Iterator ite = this.selector.selectedKeys().iterator();
			while (ite.hasNext()) {
				SelectionKey key = (SelectionKey) ite.next();
				// 删除已选的key,以防重复处理
				ite.remove();
				// 连接事件发生
				if (key.isConnectable()) {
					SocketChannel channel = (SocketChannel) key.channel();
					// 如果正在连接，则完成连接
					if (channel.isConnectionPending()) {
						channel.finishConnect();
					}
					// 设置成非阻塞
					channel.configureBlocking(false);
					// 在这里可以给服务端发送信息
					ByteBuffer buffer = ByteBuffer.allocate(1000);
					String msg = "request connect";
					buffer.put(msg.getBytes());
					// 将缓冲区各标志复位,因为向里面put了数据,标志被改变。要想从中读取数据,就要复位
					buffer.flip();
					channel.write(buffer);
					// 在和服务端连接成功之后，为了可以接收到服务端的信息，需要给通道设置读的权限。
					channel.register(this.selector, SelectionKey.OP_READ);
				} else if (key.isReadable()) {// 获得了可读的事件
					read(key);
				} else if (key.isWritable()) {
					write(key);
				}
			}
		}
	}

	/**
	 * 处理读取服务端发来的信息 的事件
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午5:51:48
	 * @param key
	 * @throws IOException
	 */
	public void read(SelectionKey key) throws IOException {

		SocketChannel channel = (SocketChannel) key.channel();
		// 创建读取的缓冲区
		ByteBuffer buffer = ByteBuffer.allocate(1000);
		channel.read(buffer);
		byte[] data = buffer.array();
		String msg = new String(data).trim();
		System.out.println("客户端收到信息：" + msg);
		channel.register(this.selector, SelectionKey.OP_WRITE);
	}
	
	/**
	 * 写数据
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午5:49:43
	 * @param key
	 * @throws IOException
	 */
	public void write(SelectionKey key) throws IOException {

		// 客户端可写入消息:得到事件发生的Socket通道
		SocketChannel channel = (SocketChannel) key.channel();
		// 创建写入的缓冲区
		ByteBuffer buffer = ByteBuffer.allocate(1000);
		String msg = "客户端信息";
		buffer.put(msg.getBytes());
		// 将缓冲区各标志复位,因为向里面put了数据,标志被改变。要想从中读取数据,就要复位
		buffer.flip();
		System.out.println("客户端发送信息：" + msg);
		channel.write(buffer);
		channel.register(this.selector, SelectionKey.OP_READ);
	}

	/**
	 * 启动客户端测试
	 * 
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		NIOClient client = new NIOClient();
		client.initClient("localhost", 8000);
		client.listen();
	}
}