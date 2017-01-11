package com.phantom.other.masterslave.session;

import java.nio.channels.SocketChannel;
import java.util.List;

import com.phantom.other.masterslave.network.NetWorkConstants;

/**
 * MS session manager
 * 
 * @author 张少奇
 * @time 2017年1月11日 下午2:51:58
 */
public interface SessionManager {

	String SESSION_RECYCLE_THREAD_NAME = "MS-Sesion-Recycle-Thread";

	int DEFAULT_PRE_INIT_SESSION_COUNT = 8;
	int DEFAULT_INCREASE_SESSION_COUNT = DEFAULT_PRE_INIT_SESSION_COUNT;
	int DEFAULT_MAX_SESSION_COUNT = 1024;

	long DEFAULT_SESSION_RECYCLE_INTERVAL = NetWorkConstants.DEFAULT_MS_HEART_BEAT_INTERVAL * 2;

	void init();

	Session newSession(SocketChannel channel);

	void freeSession(Session session);

	List<SessionState> generateSessionState();

	void destroy();
}
