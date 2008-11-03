package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public abstract class PutCommandSupport extends Command {
	private Object key;
	private Object value;

	public PutCommandSupport(byte commandId, Object key, Object value) {
		super(commandId);
		this.key = key;
		this.value = value;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		byte[] kbuf = transcoder.encode(key);
		byte[] vbuf = transcoder.encode(value);
		ByteBuffer buffer = ByteBuffer.allocate(magic.length + 4 + 4 + kbuf.length + vbuf.length);
		buffer.put(magic);
		buffer.putInt(kbuf.length);
		buffer.putInt(vbuf.length);
		buffer.put(kbuf);
		buffer.put(vbuf);
		buffer.flip();
		return buffer;
	}

	public boolean decode(ByteBuffer in) {
		if (in.remaining() < 1) {
			return false;
		}
		code = in.get();
		return true;
	}
}
