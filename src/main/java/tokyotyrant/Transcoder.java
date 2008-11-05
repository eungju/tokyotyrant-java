package tokyotyrant;

public interface Transcoder {
	byte[] encode(Object decoded);
	Object decode(byte[] encoded);
}
