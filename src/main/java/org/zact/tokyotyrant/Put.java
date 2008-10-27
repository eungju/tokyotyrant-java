package org.zact.tokyotyrant;

import org.apache.mina.core.buffer.IoBuffer;

public class Put extends Command {
	private byte[] kbuf;
	private byte[] vbuf;

	public Put(String key, String value) {
		super((byte)0x10);
		this.kbuf = key.getBytes();
		this.vbuf = value.getBytes();
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
