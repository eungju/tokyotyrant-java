package tokyotyrant;

/**
 * Always treat values as byte array.
 */
public class ByteArrayTranscoder implements Transcoder {
	public Object decode(byte[] encoded) {
		byte[] copied = new byte[encoded.length];
		System.arraycopy(encoded, 0, copied, 0, copied.length);
		return copied;
	}

	public byte[] encode(Object decoded) {
		byte[] copied = new byte[((byte[]) decoded).length];
		System.arraycopy(decoded, 0, copied, 0, copied.length);
		return copied;
	}
}
