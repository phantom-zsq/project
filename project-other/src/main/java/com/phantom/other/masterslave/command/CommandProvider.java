package com.phantom.other.masterslave.command;

import java.util.List;

/**
 * Command提供者接口，业务代码提供实现该接口的Provider，Master循环取得Command分发到Slave
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午2:58:30
 */
public interface CommandProvider {

	/**
	 * 产生Command
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午3:00:07 
	 * @return
	 */
	Command produce();

	/**
	 * 批量产生Commnad
	 * 
	 * @author 张少奇
	 * @time 2017年1月11日 下午3:00:14 
	 * @param count
	 * @return
	 */
	List<Command> produce(long count);
}
