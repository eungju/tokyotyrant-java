package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Fwmkeys extends Command {
	private Object prefix;
	private int max;
	private List<Object> keys;

	public Fwmkeys(Object prefix, int max) {
		super((byte) 0x58);
		this.prefix = prefix;
		this.max = max;
	}
	
	public List<Object> getReturnValue() {
		return isSuccess() ? keys : null;
	}

	public ByteBuffer encode() {
		byte[] pbuf = transcoder.encode(prefix);
		ByteBuffer buffer = ByteBuffer.allocate(magic.length + 4 + 4 + pbuf.length);
		buffer.put(magic);
		buffer.putInt(pbuf.length);
		buffer.putInt(max);
		buffer.put(pbuf);
		buffer.flip();
		return buffer;
	}

	public boolean decode(ByteBuffer in) {
		if (in.remaining() < 1) {
			return false;
		}
		code = in.get();

		if (in.remaining() < 4) {
			return false;
		}
		int knum = in.getInt();

		keys = new ArrayList<Object>(knum);
		for (int i = 0; i < knum; i++) {
			if (in.remaining() < 4) {
				return false;
			}
			int ksiz = in.getInt();
			if (in.remaining() < ksiz) {
				return false;
			}
			byte[] kbuf = new byte[ksiz];
			in.get(kbuf);
			keys.add(transcoder.decode(kbuf));
		}
		return true;
	}
}
