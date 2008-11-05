package tokyotyrant;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.ArrayUtils;


public class StringTranscoder implements Transcoder {
	public Object decode(byte[] encoded) {
		try {
			return new String(encoded, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unable to decode " + ArrayUtils.toString(encoded), e);
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
