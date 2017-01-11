package com.phantom.other.masterslave.session;

import java.nio.channels.SocketChannel;

public abstract class SessionFactory {

	public static Session createSession(Long id, SocketChannel channel) {
		return new Session(id, channel);
	}
}
