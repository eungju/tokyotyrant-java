package tokyotyrant.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

public class Fwmkeys extends Command<List<Object>> {
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
		byte[] pbuf = keyTranscoder.encode(prefix);
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
			keys.add(keyTranscoder.decode(kbuf));
		}
		return true;
	}

	public void encode(ChannelBuffer out) {
		byte[] pbuf = keyTranscoder.encode(prefix);
		out.writeBytes(magic);
		out.writeInt(pbuf.length);
		out.writeInt(max);
		out.writeBytes(pbuf);
	}

	public boolean decode(ChannelBuffer in) {
		if (in.readableBytes() < 1) {
			return false;
		}
		code = in.readByte();

		if (in.readableBytes() < 4) {
			return false;
		}
		int knum = in.readInt();

		keys = new ArrayList<Object>(knum);
		for (int i = 0; i < knum; i++) {
			if (in.readableBytes() < 4) {
				return false;
			}
			int ksiz = in.readInt();
			if (in.readableBytes() < ksiz) {
				return false;
			}
			byte[] kbuf = new byte[ksiz];
			in.readBytes(kbuf);
			keys.add(keyTranscoder.decode(kbuf));
		}
		return true;
	}
}
