package com.phantom.other.masterslave.command;

public interface CommandDispatcher {

	void init();

	Command dispatch(Command command);
}
