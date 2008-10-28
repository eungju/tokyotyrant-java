package org.zact.tokyotyrant;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.mina.core.buffer.IoBuffer;

public class Put extends Command {
	private byte[] kbuf;
	private byte[] vbuf;

	public Put(byte[] key, byte[] value) {
		super((byte)0x10);
		this.kbuf = key;
		this.vbuf = value;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}

	public IoBuffer encode() {
		IoBuffer buffer = IoBuffer.allocate(magic.length + 4 + 4 + kbuf.length + vbuf.length, false);
		buffer.put(magic);
		buffer.putInt(kbuf.length);
		buffer.putInt(vbuf.length);
		buffer.put(kbuf);
		buffer.put(vbuf);
		buffer.flip();
		return buffer;
	}

	public boolean decode(IoBuffer in) {
		if (in.remaining() >= 1) {
			code = in.get();
			return true;
		}
		return false;
	}
	
	public Future<Boolean> getFuture() {
		return new PutFuture(this);
	}
	
	public static class PutFuture extends CommandFuture<Put, Boolean> {
		public PutFuture(Put command) {
			super(command);
		}

		public Boolean get() throws InterruptedException, ExecutionException {
			latch.await();
			return command.isSuccess();
		}

		public Boolean get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			latch.await(timeout, unit);
			return command.isSuccess();
		}
	}
}
