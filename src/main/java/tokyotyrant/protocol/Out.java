package tokyotyrant.protocol;

import java.nio.ByteBuffer;


public class Out extends Command<Boolean> {
	private Object key;

	public Out(Object key) {
		super((byte)0x20);
		this.key = key;
	}
	
	public Boolean getReturnValue() {
		return isSuccess();
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
		return true;
	}
}