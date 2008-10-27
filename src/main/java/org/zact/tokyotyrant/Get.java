package org.zact.tokyotyrant;

import org.apache.mina.core.buffer.IoBuffer;

public class Get extends Command {
	private byte[] kbuf;
	private byte[] vbuf;

	public Get(String key) {
		super((byte)0x30);
		this.kbuf = key.getBytes();
	}
	
	public boolean isSuccess() {
		return code == 0;
	}
	
	public String getValue() {
		return new String(vbuf);
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
}
