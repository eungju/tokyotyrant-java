package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.lang.ArrayUtils;

public class IntegerTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public IntegerTranscoder() {
		this(ByteOrder.nativeOrder());
	}
	
	public IntegerTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}
	
	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(Integer.SIZE / 8).order(byteOrder).putInt((Integer) decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != Integer.SIZE / 8) {
			throw new IllegalArgumentException("Unable to decode " + ArrayUtils.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getInt();
	}
}
