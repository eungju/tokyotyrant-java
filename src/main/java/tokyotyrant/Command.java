package tokyotyrant;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public abstract class Command<T> {
	protected Transcoder transcoder;
	protected byte[] magic;
	protected byte code = (byte) 0xff;
	
	private CommandState state = CommandState.WRITING;
	private CountDownLatch latch = new CountDownLatch(1);
	private boolean cancelled = false;
	private Exception errorException = null; 
	
	public Command(byte commandId) {
		magic = new byte[] {(byte) 0xC8, commandId};
	}
	
	public void setTranscoder(Transcoder transcoder) {
		this.transcoder = transcoder;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}
	
	public CommandState getState() {
		return state;
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	public void reading() {
		state = CommandState.READING;
	}

	/**
	 * Should be invoked when the command is cancelled.
	 */
	public void cancel() {
		latch.countDown();
		cancelled = true;
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	
	/**
	 * Should be invoked when the command is completed.
	 */
	public void complete() {
		latch.countDown();
		state = CommandState.COMPLETE;
	}
	
	/**
	 * Should be invoked when the command is not completed by an error.
	 *
	 * @param exception
	 */
	public void error(Exception exception) {
		latch.countDown();
		errorException = exception;
	}
	
	public boolean hasError() {
		return errorException != null;
	}
	
	public Exception getErrorException() {
		return errorException;
	}
	
	public abstract T getReturnValue();

	public abstract ByteBuffer encode();

	public abstract boolean decode(ByteBuffer in);
}
