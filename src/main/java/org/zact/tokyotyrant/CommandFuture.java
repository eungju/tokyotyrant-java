package org.zact.tokyotyrant;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public abstract class CommandFuture<C extends Command, T> implements Future<T> {
	protected C command;
	protected CountDownLatch latch;
	
	public CommandFuture(C command) {
		this.command = command;
		this.latch = command.getLatch();
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isDone() {
		// TODO Auto-generated method stub
		return false;
	}
}
