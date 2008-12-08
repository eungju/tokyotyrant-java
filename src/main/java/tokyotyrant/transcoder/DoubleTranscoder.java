package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleTranscoder implements Transcoder {
	private ByteOrder byteOrder;

	public DoubleTranscoder() {
		this(ByteOrder.nativeOrder());
	}
	
	public DoubleTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(8).order(byteOrder).putDouble((Double)decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != 8) {
			throw new IllegalArgumentException("It's not a double. Doubles are 8 bytes");
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getDouble();
	}
}
