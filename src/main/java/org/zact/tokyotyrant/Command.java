package org.zact.tokyotyrant;

import java.util.concurrent.CountDownLatch;

import org.apache.mina.core.buffer.IoBuffer;

public abstract class Command {
	protected byte[] magic;
	protected byte code;
	private CountDownLatch latch;
	
	public Command(byte commandId) {
		magic = new byte[] {(byte) 0xC8, commandId};
		latch = new CountDownLatch(1);
	}
	
	public CountDownLatch getLatch() {
		return latch;
	}
	
	public void completed() {
		latch.countDown();
	}
	
	public abstract IoBuffer encode();
	public abstract boolean decode(IoBuffer in);
	public abstract boolean isSuccess();
}
