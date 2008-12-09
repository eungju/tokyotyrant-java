package tokyotyrant.transcoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

public class SerializingTranscoder implements Transcoder {
	static final byte TYPE_STRING = 0;
	static final byte TYPE_BOOLEAN = 1;
	static final byte TYPE_INTEGER = 2;
	static final byte TYPE_LONG = 3;
	static final byte TYPE_DATE = 4;
	static final byte TYPE_BYTE = 5;
	static final byte TYPE_FLOAT = 6;
	static final byte TYPE_DOUBLE = 7;
	static final byte TYPE_BYTEARRAY = 8;
	static final byte TYPE_SERIALIZABLE = Byte.MAX_VALUE;

    /**
     * Use network byte order.
     */
	private final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
	private final StringTranscoder stringTranscoder = new StringTranscoder();
    private final ByteTranscoder byteTranscoder = new ByteTranscoder();
	private final IntegerTranscoder integerTranscoder = new IntegerTranscoder(byteOrder);
	private final LongTranscoder longTranscoder = new LongTranscoder(byteOrder);
	private final FloatTranscoder floatTranscoder = new FloatTranscoder(byteOrder);
	private final DoubleTranscoder doubleTranscoder = new DoubleTranscoder(byteOrder);
    private final ByteArrayTranscoder byteArrayTranscoder = new ByteArrayTranscoder();
	private final SerializableTranscoder serializableTranscoder = new SerializableTranscoder();
	
	public byte[] encode(Object decoded) {
		byte typeFlag;
		byte[] body;
		if (decoded instanceof String) {
			typeFlag = TYPE_STRING;
			body = stringTranscoder.encode(decoded);
		} else if (decoded instanceof Boolean) {
			typeFlag = TYPE_BOOLEAN;
			body = byteTranscoder.encode(((Boolean)decoded ? 1 : 0));
		} else if (decoded instanceof Integer) {
			typeFlag = TYPE_INTEGER;
			body = integerTranscoder.encode(decoded);
		} else if (decoded instanceof Long) {
			typeFlag = TYPE_LONG;
			body = longTranscoder.encode(decoded);
		} else if (decoded instanceof Date) {
			typeFlag = TYPE_DATE;
			body = longTranscoder.encode(((Date)decoded).getTime());
		} else if (decoded instanceof Byte) {
			typeFlag = TYPE_BYTE;
			body = byteTranscoder.encode(decoded);
		} else if (decoded instanceof Float) {
			typeFlag = TYPE_FLOAT;
			body = floatTranscoder.encode(decoded);
		} else if (decoded instanceof Double) {
			typeFlag = TYPE_DOUBLE;
			body = doubleTranscoder.encode(decoded);
		} else if (decoded instanceof byte[]) {
			typeFlag = TYPE_BYTEARRAY;
			body = byteArrayTranscoder.encode(decoded);
		} else {
			typeFlag = TYPE_SERIALIZABLE;
			body = serializableTranscoder.encode(decoded);
		}
		ByteBuffer buf = ByteBuffer.allocate(1 + body.length);
		buf.put(typeFlag);
		buf.put(body);
		return buf.array();
	}

	public Object decode(byte[] encoded) {
		Object decoded;
		ByteBuffer buf = ByteBuffer.wrap(encoded);
		byte typeFlag = buf.get();
		byte[] body = new byte[buf.remaining()];
		System.arraycopy(buf.array(), 1, body, 0, body.length);
		switch (typeFlag) {
		case TYPE_STRING:
			decoded = stringTranscoder.decode(body);
			break;
		case TYPE_BOOLEAN:
			decoded = (Byte)byteTranscoder.decode(body) == 0 ? false : true;
			break;
		case TYPE_INTEGER:
			decoded = integerTranscoder.decode(body);
			break;
		case TYPE_LONG:
			decoded = longTranscoder.decode(body);
			break;
		case TYPE_DATE:
			decoded = new Date((Long)longTranscoder.decode(body));
			break;
		case TYPE_BYTE:
			decoded = byteTranscoder.decode(body);
			break;
		case TYPE_FLOAT:
			decoded = floatTranscoder.decode(body);
			break;
		case TYPE_DOUBLE:
			decoded = doubleTranscoder.decode(body);
			break;
		case TYPE_BYTEARRAY:
			decoded = byteArrayTranscoder.decode(body);
			break;
		default:
			decoded = serializableTranscoder.decode(body);
		}
		return decoded;
	}
}
