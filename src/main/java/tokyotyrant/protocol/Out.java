package tokyotyrant.protocol;

import org.jboss.netty.buffer.ChannelBuffer;

public class Out extends Command<Boolean> {
	private Object key;

	public Out(Object key) {
		super((byte)0x20);
		this.key = key;
	}
	
	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	public void encode(ChannelBuffer out) {
		byte[] kbuf = keyTranscoder.encode(key);
		out.writeBytes(magic);
		out.writeInt(kbuf.length);
		out.writeBytes(kbuf);
	}

	public boolean decode(ChannelBuffer in) {
		if (in.readableBytes() < 1) {
			return false;
		}
		code = in.readByte();
		return true;
	}
}
