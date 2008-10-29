package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Rnum extends Command {
	private long rnum;

	public Rnum() {
		super((byte)0x80);
	}
	
	public long getReturnValue() {
		return isSuccess() ? rnum : 0;
	}

	public ByteBuffer encode() {
		ByteBuffer buffer = ByteBuffer.allocate(magic.length);
		buffer.put(magic);
		buffer.flip();
		return buffer;
	}

	public boolean decode(ByteBuffer in) {
		if (in.remaining() < 1) {
			return false;
		}
		code = in.get();
		
		if (in.remaining() < 8) {
			return false;
		}
		rnum = in.getLong();
		return true;
	}
}
