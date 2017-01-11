package com.phantom.other.masterslave.command;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.phantom.other.masterslave.Node;
import com.phantom.other.masterslave.master.MasterState;

public class SlaveHeartbeatCommandHandler implements CommandHandler {

	private static final Log log = LogFactory.getLog(SlaveHeartbeatCommandHandler.class);

	private Node slave;

	@Override
	public Command handle(Command command) {
		log.info("Slave received heartbeat from master");
		slave.acceptStatistics(MasterState.fromByteBuffer(command.getPayLoad()));
		return null;
		// return
		// CommandFactory.createHeartbeatCommand(((SlaveState)slave.gatherStatistics()).toByteBuffer());
	}

	public void setSlave(Node slave) {
		this.slave = slave;
	}
}
