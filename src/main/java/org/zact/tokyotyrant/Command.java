package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public abstract class Command {
	protected Transcoder transcoder;
	protected byte[] magic;
	protected byte code;
	
	public Command(byte commandId) {
		magic = new byte[] {(byte) 0xC8, commandId};
	}
	
	public void setTranscoder(Transcoder transcoder) {
		this.transcoder = transcoder;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}

	public abstract ByteBuffer encode();

	public abstract boolean decode(ByteBuffer in);
}
