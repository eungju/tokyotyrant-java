package org.zact.tokyotyrant;

import java.util.concurrent.CountDownLatch;

import org.apache.mina.core.buffer.IoBuffer;

public class Put extends Command {
	private byte[] kbuf;
	private byte[] vbuf;

	public Put(CountDownLatch latch, byte[] key, byte[] value) {
		super(latch, (byte)0x10);
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
}
