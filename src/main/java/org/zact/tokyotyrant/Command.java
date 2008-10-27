package org.zact.tokyotyrant;

import java.util.concurrent.CountDownLatch;

import org.apache.mina.core.buffer.IoBuffer;

public abstract class Command {
	protected byte[] magic;
	protected byte code;
	private CountDownLatch latch;
	
	public Command(CountDownLatch latch, byte commandId) {
		this.latch = latch;
		magic = new byte[] {(byte) 0xC8, commandId};
	}
	
	public abstract IoBuffer encode();
	public abstract boolean decode(IoBuffer in);
	public abstract boolean isSuccess();
	public void completed() {
		latch.countDown();
	}
}
