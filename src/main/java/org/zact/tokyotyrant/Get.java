package org.zact.tokyotyrant;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.mina.core.buffer.IoBuffer;

public class Get extends Command {
	private byte[] kbuf;
	private byte[] vbuf;

	public Get(byte[] key) {
		super((byte)0x30);
		this.kbuf = key;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}
	
	public String getValue() {
		return isSuccess() ? new String(vbuf) : null;
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
			if (isSuccess() && in.prefixedDataAvailable(4)) {
				int vsiz = in.getInt();
				vbuf = new byte[vsiz];
				in.get(vbuf);
			}
			return true;
		}
		return false;
	}
	
	public Future<Object> getFuture() {
		return new GetFuture(this);
	}
	
	public static class GetFuture extends CommandFuture<Get, Object> {
		public GetFuture(Get command) {
			super(command);
		}

		public Object get() throws InterruptedException, ExecutionException {
			latch.await();
			return command.getValue();
		}

		public Object get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			latch.await(timeout, unit);
			return command.getValue();
		}
	}	
}
