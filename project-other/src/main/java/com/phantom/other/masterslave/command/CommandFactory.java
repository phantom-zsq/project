package com.phantom.other.masterslave.command;

import java.nio.ByteBuffer;

import com.phantom.other.masterslave.network.NetWorkConstants;
import com.phantom.other.masterslave.session.Session;

/**
 * Command工厂，负责创建Command
 * 
 * 0号Command保留给HeartbeatCommand
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午2:58:43
 */
public class CommandFactory {
	
	public static Command createCommand(Session session, Long type, ByteBuffer payLoad) {
		Command command = new Command(session, type, payLoad);
		return command;
	}

	public static Command createCommand(Long type, ByteBuffer payLoad) {
		Command command = new Command(type, payLoad);
		return command;
	}

	public static Command createHeartbeatCommand(Session session) {
		byte[] bytes = NetWorkConstants.MS_HEART_BEAT_MSG.getBytes();
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
		buffer.put(bytes);
		buffer.flip();
		return CommandFactory.createCommand(session, Command.HEART_BEAT_COMMAND, buffer);
	}

	public static Command createHeartbeatCommand(ByteBuffer payLoad) {
		return CommandFactory.createCommand(Command.HEART_BEAT_COMMAND, payLoad);
	}

	public static Command createHeartbeatCommand() {
		byte[] bytes = NetWorkConstants.MS_HEART_BEAT_MSG.getBytes();
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
		buffer.put(bytes);
		buffer.flip();
		return CommandFactory.createCommand(Command.HEART_BEAT_COMMAND, buffer);
	}
}
