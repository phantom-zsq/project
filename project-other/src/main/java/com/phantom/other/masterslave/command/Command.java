package com.phantom.other.masterslave.command;

import java.nio.ByteBuffer;

import com.phantom.other.masterslave.session.Session;

public class Command {

	public static Long HEART_BEAT_COMMAND = 0L;

	private Long type;

	private ByteBuffer payLoad;

	private Session session;

	public Command(Long type, ByteBuffer payLoad) {
		this(null, type, payLoad);
	}

	public Command(Session session, Long type, ByteBuffer payLoad) {
		this.session = session;
		this.type = type;
		this.payLoad = payLoad;
	}

	public void setType(Long type) {
		this.type = type;
	}

	public Long getType() {
		return type;
	}

	public void setPayLoad(ByteBuffer payLoad) {
		this.payLoad = payLoad;
	}

	public ByteBuffer getPayLoad() {
		return payLoad;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}
}
