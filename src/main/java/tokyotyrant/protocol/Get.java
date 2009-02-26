package tokyotyrant.protocol;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import tokyotyrant.helper.BufferHelper;

public class Get extends Command<Object> {
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
		byte[] kbuf = keyTranscoder.encode(key);
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
		if (!BufferHelper.prefixedDataAvailable(in, 4)) {
			return false;
		}
		int vsiz = in.getInt();
		byte[] vbuf = new byte[vsiz];
		in.get(vbuf);
		value = valueTranscoder.decode(vbuf);
		return true;
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
		if (!isSuccess()) {
			return true;
		}
		if (!BufferHelper.prefixedDataAvailable(in, 4)) {
			return false;
		}
		int vsiz = in.readInt();
		byte[] vbuf = new byte[vsiz];
		in.readBytes(vbuf);
		value = valueTranscoder.decode(vbuf);
		return true;
	}
}
