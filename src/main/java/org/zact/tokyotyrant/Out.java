package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Out extends Command {
	private Object key;

	public Out(Object key) {
		super((byte)0x20);
		this.key = key;
	}
	
	public boolean isSuccess() {
		return code == 0;
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
		if (in.remaining() >= 1) {
			code = in.get();
			return true;
		}
		return false;
	}
}
