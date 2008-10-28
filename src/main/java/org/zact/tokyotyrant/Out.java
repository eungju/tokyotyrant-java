package org.zact.tokyotyrant;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.mina.core.buffer.IoBuffer;

public class Out extends Command {
	private byte[] kbuf;

	public Out(byte[] key) {
		super((byte)0x20);
		this.kbuf = key;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}

	public IoBuffer encode() {
		IoBuffer buffer = IoBuffer.allocate(magic.length + 4 + kbuf.length, false);
		buffer.put(magic);
		buffer.putInt(kbuf.length);
		buffer.put(kbuf);
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
		return new OutFuture(this);
	}
	
	public static class OutFuture extends CommandFuture<Out, Boolean> {
		public OutFuture(Out command) {
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
