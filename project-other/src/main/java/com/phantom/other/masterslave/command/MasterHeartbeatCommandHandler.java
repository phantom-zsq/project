package com.phantom.other.masterslave.command;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.phantom.other.masterslave.Node;
import com.phantom.other.masterslave.master.MasterState;
import com.phantom.other.masterslave.slave.SlaveState;

public class MasterHeartbeatCommandHandler implements CommandHandler {

	private static final Log log = LogFactory.getLog(MasterHeartbeatCommandHandler.class);

	private Node master;

	@Override
	public Command handle(Command command) {
		log.info(new StringBuilder("Master receive heartbeat from slave : ")
				.append(command.getSession().getRemoteIP().toString()));
		command.getSession().alive();
		master.acceptStatistics(SlaveState.fromByteBuffer(command.getPayLoad()));
		return CommandFactory.createHeartbeatCommand(((MasterState) master.gatherStatistics()).toByteBuffer());
	}

	public void setMaster(Node master) {
		this.master = master;
	}
}
