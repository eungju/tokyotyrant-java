package tokyotyrant.protocol;

import tokyotyrant.transcoder.Transcoder;

public class Iternext extends BinaryCommandSupport<Object> {
	private static final PacketFormat REQUEST = magic().end();
	private static final PacketFormat RESPONSE = code(true).int32("ksiz").bytes("kbuf", "ksiz").end();
	private byte[] key;
	
	public Iternext(Transcoder keyTranscoder, Transcoder valueTranscoder) {
		super((byte) 0x51, REQUEST, RESPONSE, keyTranscoder, valueTranscoder);
	}
	
	public Object getReturnValue() {
		return isSuccess() ? keyTranscoder.decode(key) : null;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte) context.get("code");
		if (code == ESUCCESS) {
			key = (byte[]) context.get("kbuf");
		}
	}
}
