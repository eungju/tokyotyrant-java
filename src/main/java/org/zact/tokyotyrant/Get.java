package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Get extends Command {
	private Object key;
	private Object value;

	public Get(Object key) {
		super((byte)0x30);
		this.key = key;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}
	
	public Object getValue() {
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
		if (in.remaining() >= 1) {
			code = in.get();
			if (isSuccess() && prefixedDataAvailable(in, 4)) {
				int vsiz = in.getInt();
				byte[] vbuf = new byte[vsiz];
				in.get(vbuf);
				value = transcoder.decode(vbuf);
			}
			return true;
		}
		return false;
	}
}
