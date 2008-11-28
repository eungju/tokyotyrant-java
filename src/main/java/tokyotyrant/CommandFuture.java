package tokyotyrant;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CommandFuture<T> implements Future<T> {
	private final Command<T> command;
	private final CountDownLatch latch;
	private long globalOperationTimeout = Long.MAX_VALUE;

	public CommandFuture(Command<T> command) {
		this.command = command;
		this.latch = command.getLatch();
	}

	public T get() throws InterruptedException, ExecutionException {
		try {
			return get(globalOperationTimeout , TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new RuntimeException("Timed out waiting for operation", e);
		}
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (!latch.await(timeout, unit)) {
			throw new TimeoutException("Timed out waiting for operation");
		}
		if (command != null && command.hasError()) {
			throw new ExecutionException(command.getErrorException());
		}
		if (isCancelled()) {
			throw new ExecutionException(new RuntimeException("Cancelled"));
		}
		return (T) command.getReturnValue();
	}
	
	public boolean cancel(boolean mayInterruptIfRunning) {
		command.cancel();
		// This isn't exactly correct, but it's close enough.  If we're in
		// a writing state, we *probably* haven't started.
		return command.getState() == CommandState.WRITING;
	}

	public boolean isCancelled() {
		return command.isCancelled();
	}

	public boolean isDone() {
		return command.getState() == CommandState.COMPLETE || command.hasError() ||command.isCancelled();
	}
}
