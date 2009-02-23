package tokyotyrant.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.builder.ToStringBuilder;

import tokyotyrant.transcoder.Transcoder;

public abstract class Command<T> {
	public static final byte ESUCCESS = 0x00;
	public static final byte EUNKNOWN = (byte) 0xff;
	protected byte[] magic;
	protected byte code = EUNKNOWN;
	
	protected Transcoder keyTranscoder;
	protected Transcoder valueTranscoder;

	private transient CountDownLatch latch = new CountDownLatch(1);
	private transient CommandState state = CommandState.WRITING;
	private transient Exception errorException = null; 
	
	public Command(byte commandId) {
		magic = new byte[] {(byte) 0xC8, commandId};
	}
	
	public void setKeyTranscoder(Transcoder transcoder) {
		this.keyTranscoder = transcoder;
	}
	
	public void setValueTranscoder(Transcoder transcoder) {
		this.valueTranscoder = transcoder;
	}
	
	public boolean isSuccess() {
		return code == ESUCCESS;
	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	public CountDownLatch getLatch() {
		return latch;
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
	
	public abstract T getReturnValue();

	public abstract ByteBuffer encode();

	public abstract boolean decode(ByteBuffer in);
}
