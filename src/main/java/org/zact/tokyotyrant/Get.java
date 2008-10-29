package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Get extends Command {
	private Object key;
	private Object value;

	public Get(Object key) {
		super((byte)0x30);
		this.key = key;
	}
	
	public Object getReturnValue() {
		return isSuccess() ? value : null;
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
		if (!prefixedDataAvailable(in, 4)) {
			return false;
		}
		int vsiz = in.getInt();
		byte[] vbuf = new byte[vsiz];
		in.get(vbuf);
		value = transcoder.decode(vbuf);
		return true;
	}
}
