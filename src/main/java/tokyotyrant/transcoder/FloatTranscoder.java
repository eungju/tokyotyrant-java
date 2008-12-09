package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.lang.ArrayUtils;

public class FloatTranscoder implements Transcoder {
	private final ByteOrder byteOrder;

	public FloatTranscoder() {
		this(ByteOrder.nativeOrder());
	}
	
	public FloatTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(4).order(byteOrder).putFloat((Float)decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != 4) {
			throw new IllegalArgumentException("Unable to decode " + ArrayUtils.toString(encoded));
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getFloat();
	}
}
