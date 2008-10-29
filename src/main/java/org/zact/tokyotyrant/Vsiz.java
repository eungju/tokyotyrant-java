package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Vsiz extends Command {
	private Object key;
	private int vsiz;

	public Vsiz(Object key) {
		super((byte)0x38);
		this.key = key;
	}
	
	public int getValue() {
		return isSuccess() ? vsiz : -1;
	}

	public ByteBuffer encode() {
		byte[] kbuf = transcoder.encode(key);
		ByteBuffer buffer = ByteBuffer.allocate(magic.length + 4 + kbuf.length);
		buffer.put(magic);
		buffer.putInt(kbuf.length);
		buffer.put(kbuf);
		buffer.flip();
		return buffer;
	}

	public boolean decode(ByteBuffer in) {
		if (in.remaining() < 1) {
			return false;
		}
		code = in.get();
		if (!isSuccess()) {
			return true;
		}
		if (in.remaining() < 4) {
			return false;
		}
		vsiz = in.getInt();
		return true;
	}
}
