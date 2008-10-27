package org.zact.tokyotyrant;

import org.apache.mina.core.buffer.IoBuffer;

public class Out extends Command {
	private byte[] kbuf;

	public Out(String key) {
		super((byte)0x20);
		this.kbuf = key.getBytes();
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
}
