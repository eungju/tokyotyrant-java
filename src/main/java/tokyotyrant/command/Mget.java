package tokyotyrant.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import tokyotyrant.Command;

public class Mget extends Command<Map<Object, Object>> {
	private Object[] keys;
	private Map<Object, Object> values;

	public Mget(Object[] keys) {
		super((byte)0x31);
		this.keys = keys;
	}
	
	public Map<Object, Object> getReturnValue() {
		return isSuccess() ? values : null;
	}

	public ByteBuffer encode() {
		int capacity = magic.length + 4;
		byte[][] kbufs = new byte[keys.length][];
		for (int i = 0; i < keys.length; i++) {
			kbufs[i] = keyTranscoder.encode(keys[i]);
			capacity += 4 + kbufs[i].length;
		}
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		buffer.put(magic);
		buffer.putInt(keys.length);
		for (byte[] each : kbufs) {
			buffer.putInt(each.length);
			buffer.put(each);
		}
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
		int rnum = in.getInt();

		values = new HashMap<Object, Object>(rnum);
		for (int i = 0; i < rnum; i++) {
			if (in.remaining() < 4 + 4) {
				return false;
			}
			int ksiz = in.getInt();
			int vsiz = in.getInt();
			if (in.remaining() < ksiz + vsiz) {
				return false;
			}
			byte[] kbuf = new byte[ksiz];
			in.get(kbuf);
			byte[] vbuf = new byte[vsiz];
			in.get(vbuf);
			values.put(keyTranscoder.decode(kbuf), valueTranscoder.decode(vbuf));
		}
		return true;
	}
}
