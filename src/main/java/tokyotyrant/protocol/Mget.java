package tokyotyrant.protocol;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

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

	public void encode(ChannelBuffer out) {
		out.writeBytes(magic);
		out.writeInt(keys.length);
		for (Object each : keys) {
			byte[] kbuf = keyTranscoder.encode(each);
			out.writeInt(kbuf.length);
			out.writeBytes(kbuf);
		}
	}

	public boolean decode(ChannelBuffer in) {
		if (in.readableBytes() < 1) {
			return false;
		}
		code = in.readByte();

		if (in.readableBytes() < 4) {
			return false;
		}
		int rnum = in.readInt();

		values = new HashMap<Object, Object>(rnum);
		for (int i = 0; i < rnum; i++) {
			if (in.readableBytes() < 4 + 4) {
				return false;
			}
			int ksiz = in.readInt();
			int vsiz = in.readInt();
			if (in.readableBytes() < ksiz + vsiz) {
				return false;
			}
			byte[] kbuf = new byte[ksiz];
			in.readBytes(kbuf);
			byte[] vbuf = new byte[vsiz];
			in.readBytes(vbuf);
			values.put(keyTranscoder.decode(kbuf), valueTranscoder.decode(vbuf));
		}
		return true;
	}
}
