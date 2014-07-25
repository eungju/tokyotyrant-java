package tokyotyrant.protocol;

import java.util.concurrent.CountDownLatch;

import org.jboss.netty.buffer.ChannelBuffer;

public abstract class Command<T> {
	private CountDownLatch latch;
	private CommandState state;
	private Exception errorException; 

	public abstract T getReturnValue();

	public abstract void encode(ChannelBuffer out);

    public abstract boolean responseRequired();

    public abstract boolean decode(ChannelBuffer in);

	public CommandFuture<T> writing(long timeout) {
		latch = new CountDownLatch(1);
		state = CommandState.WRITING;
		errorException = null; 
		return new CommandFuture<T>(this, latch, timeout);
	}
	
	public boolean isWriting() {
		return state == CommandState.WRITING;
	}

	public void reading() {
		state = CommandState.READING;
	}
	
	public boolean isReading() {
		return state == CommandState.READING;
	}
	
	/**
	 * Should be invoked when the command is completed.
	 */
	public void complete() {
		latch.countDown();
		state = CommandState.COMPLETE;
	}
	
	public boolean isCompleted() {
		return state == CommandState.COMPLETE;
	}

	/**
	 * Should be invoked when the command is cancelled.
	 */
	public void cancel() {
		latch.countDown();
		state = CommandState.CANCELLED;
	}
	
	public boolean isCancelled() {
		return state == CommandState.CANCELLED;
	}
	
	/**
	 * Should be invoked when the command is not completed by an error.
	 *
	 * @param exception
	 */
	public void error(Exception exception) {
		latch.countDown();
		state = CommandState.ERROR;
		errorException = exception;
	}
	
	public boolean hasError() {
		return state == CommandState.ERROR;
	}
	
	public Exception getErrorException() {
		return errorException;
	}
}
