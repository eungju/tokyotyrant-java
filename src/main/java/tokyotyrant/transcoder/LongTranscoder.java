package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.lang.ArrayUtils;

public class LongTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public LongTranscoder() {
		this(ByteOrder.nativeOrder());
	}
	
	public LongTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}
	
	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(8).order(byteOrder).putLong((Long)decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != 8) {
			throw new IllegalArgumentException("Unable to decode " + ArrayUtils.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getLong();
	}
}
