package tokyotyrant.protocol;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import tokyotyrant.transcoder.Transcoder;

public abstract class Command<T> {
	public static final byte ESUCCESS = 0x00;
	public static final byte EUNKNOWN = (byte) 0xff;
	protected byte[] magic;
	protected byte code = EUNKNOWN;
	
	protected Transcoder keyTranscoder;
	protected Transcoder valueTranscoder;

	private CountDownLatch latch = new CountDownLatch(1);
	private CommandState state = CommandState.WRITING;
	private Exception errorException = null; 
	
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

	/*
	public ByteBuffer encode() {
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		encode(buffer);
		ByteBuffer out = ByteBuffer.allocate(buffer.readableBytes());
		buffer.readBytes(out);
		out.flip();
		return out;
	}

	public boolean decode(ByteBuffer in) {
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(in);
		int index = buffer.readerIndex();
		boolean completed = decode(buffer);
		in.position(in.position() + (buffer.readerIndex() - index));
		return completed;
	}
	*/

	public abstract void encode(ChannelBuffer out);

	public abstract boolean decode(ChannelBuffer in);
}
