package tokyotyrant.protocol;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import tokyotyrant.transcoder.Transcoder;

public class Fwmkeys extends Command<List<Object>> {
	private final byte[] prefix;
	private final int max;
	private List<byte[]> keys;

	public Fwmkeys(Transcoder keyTranscoder, Transcoder valueTranscoder, Object prefix, int max) {
		super((byte) 0x58, keyTranscoder, valueTranscoder);
		this.prefix = keyTranscoder.encode(prefix);
		this.max = max;
	}
	
	public List<Object> getReturnValue() {
		if (!isSuccess()) {
			return null;
		}
		List<Object> result = new ArrayList<Object>(keys.size());
		for (byte[] kbuf : keys) {
			result.add(valueTranscoder.decode(kbuf));
		}
		return result;
	}

	public void encode(ChannelBuffer out) {
		out.writeBytes(magic);
		out.writeInt(prefix.length);
		out.writeInt(max);
		out.writeBytes(prefix);
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

		keys = new ArrayList<byte[]>(knum);
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
			keys.add(kbuf);
		}
		return true;
	}
}
