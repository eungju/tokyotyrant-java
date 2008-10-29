package org.zact.tokyotyrant;

import java.io.UnsupportedEncodingException;


public class StringTranscoder implements Transcoder {
	public Object decode(byte[] encoded) {
		try {
			return new String(encoded, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unable to decode " + encoded, e);
		}
	}

	public byte[] encode(Object decoded) {
		try {
			return decoded.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unable to encode " + decoded, e);
		}
	}
}
