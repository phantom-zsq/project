package com.phantom.other.masterslave.command;

public interface CommandHandler {

	/**
	 * 命令处理器
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午2:59:47 
	 * @param command
	 * @return
	 */
	Command handle(Command command);

	// Command handle(Command command, CommandCompletedCallback callback);
}
