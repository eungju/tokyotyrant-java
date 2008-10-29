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
	
    protected boolean prefixedDataAvailable(ByteBuffer in, int prefixLength) {
    	return prefixedDataAvailable(in, prefixLength, Integer.MAX_VALUE);
    }

    protected boolean prefixedDataAvailable(ByteBuffer in, int prefixLength, int maxDataLength) {
        if (in.remaining() < prefixLength) {
            return false;
        }

        int dataLength;
        switch (prefixLength) {
        case 1:
            dataLength = in.get(in.position()) & 0xff;
            break;
        case 2:
            dataLength = in.getShort(in.position()) & 0xffff;
            break;
        case 4:
            dataLength = in.getInt(in.position()) & 0xffffffff;
            break;
        default:
            throw new IllegalArgumentException("prefixLength: " + prefixLength);
        }

        if (dataLength < 0 || dataLength > maxDataLength) {
            throw new IllegalStateException("dataLength: " + dataLength);
        }

        return in.remaining() - prefixLength >= dataLength;
	}
	
	public boolean isSuccess() {
		return code == 0;
	}

	public abstract ByteBuffer encode();

	public abstract boolean decode(ByteBuffer in);
}
