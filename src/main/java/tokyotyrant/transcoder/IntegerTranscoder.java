package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntegerTranscoder implements Transcoder {
	private ByteOrder byteOrder;

	public IntegerTranscoder() {
		this(ByteOrder.nativeOrder());
	}
	
	public IntegerTranscoder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}
	
	public byte[] encode(Object decoded) {
		return ByteBuffer.allocate(4).order(byteOrder).putInt((Integer)decoded).array();
	}

	public Object decode(byte[] encoded) {
		if (encoded.length != 4) {
			throw new IllegalArgumentException("It's not an integer. Integers are 4 bytes");
		}
		return ByteBuffer.wrap(encoded).order(byteOrder).getInt();
	}
}
