package com.phantom.other.masterslave.slave;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.phantom.other.masterslave.Node;
import com.phantom.other.masterslave.State;
import com.phantom.other.masterslave.command.Command;
import com.phantom.other.masterslave.command.CommandProvider;
import com.phantom.other.masterslave.config.MSConfig;
import com.phantom.other.masterslave.master.MasterState;
import com.phantom.other.masterslave.network.NetWorkClient;
import com.phantom.other.masterslave.slave.failure.slave.MasterFailureMonitor;

public class SlaveNode implements Node {

	private static final Log log = LogFactory.getLog(SlaveNode.class);

	private volatile boolean runningFlag = true;

	private NetWorkClient netWorkClient;

	private AtomicLong stateIDGenrator = new AtomicLong(0);

	private CommandProvider commandProvider;

	private Thread slaveThread;

	private Boolean isMasterCandidate;

	private MasterFailureMonitor masterFailureMonitor;

	private MSConfig msConfig;

	@Override
	public void init() {
		netWorkClient.init();
		masterFailureMonitor.init();
		slaveThread = new Thread(this, "MS-Slave-Thread");
	}

	@Override
	public void start() {
		runningFlag = true;
		netWorkClient.start();
		masterFailureMonitor.start();
		slaveThread.start();
	}

	@Override
	public void stop() {
		runningFlag = false;
		netWorkClient.stop();
		masterFailureMonitor.stop();
	}

	@Override
	public void reset() {
		
	}

	@Override
	public void run() {
		try {
			while (runningFlag) {
				// sleep
				Thread.sleep(msConfig.getSlaveCommandProduceInterval() * 1000);

				Command command = commandProvider.produce();
				if (command != null) {
					netWorkClient.send(command);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			stop();
		}
	}

	@Override
	public State gatherStatistics() {
		SlaveState slaveState = new SlaveState();
		slaveState.setId(stateIDGenrator.addAndGet(1));
		slaveState.setIsMasterCandidate(isMasterCandidate);

		return slaveState;
	}

	@Override
	public void acceptStatistics(State state) {
		log.info(new StringBuilder("Slave Accept Master state : ").append(((MasterState) state).toString()).toString());
		masterFailureMonitor.process((MasterState) state);
	}

	public void setNetWorkClient(NetWorkClient netWorkClient) {
		this.netWorkClient = netWorkClient;
	}

	public void setCommandProvider(CommandProvider commandProvider) {
		this.commandProvider = commandProvider;
	}

	public void setIsMasterCandidate(Boolean isMasterCandidate) {
		this.isMasterCandidate = isMasterCandidate;
	}

	public void setMasterFailureMonitor(MasterFailureMonitor masterFailureMonitor) {
		this.masterFailureMonitor = masterFailureMonitor;
	}

	public void setMsConfig(MSConfig msConfig) {
		this.msConfig = msConfig;
	}
}
