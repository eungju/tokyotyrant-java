package org.zact.tokyotyrant;

import org.apache.mina.core.buffer.IoBuffer;

public abstract class Command {
	protected byte[] magic;
	protected byte code;

	public Command(byte commandId) {
		magic = new byte[] {(byte) 0xC8, commandId};
	}
	
	public abstract IoBuffer encode();
	public abstract boolean decode(IoBuffer in);
	public abstract boolean isSuccess();
}
