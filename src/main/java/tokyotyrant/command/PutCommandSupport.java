package tokyotyrant.command;

import java.nio.ByteBuffer;

import tokyotyrant.Command;

public abstract class PutCommandSupport extends Command<Boolean> {
	private Object key;
	private Object value;

	public PutCommandSupport(byte commandId, Object key, Object value) {
		super(commandId);
		this.key = key;
		this.value = value;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		byte[] kbuf = keyTranscoder.encode(key);
		byte[] vbuf = valueTranscoder.encode(value);
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
