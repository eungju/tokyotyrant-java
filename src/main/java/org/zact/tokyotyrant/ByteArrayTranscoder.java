package org.zact.tokyotyrant;

public class ByteArrayTranscoder implements Transcoder {
	public Object decode(byte[] encoded) {
		return encoded;
	}

	public byte[] encode(Object decoded) {
		return (byte[]) decoded;
	}
}
