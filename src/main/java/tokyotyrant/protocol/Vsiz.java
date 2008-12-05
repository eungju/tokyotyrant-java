package tokyotyrant.protocol;

public class Vsiz extends CommandSupport<Integer> {
	private static final PacketFormat REQUEST = magic().int32("ksiz").bytes("kbuf", "ksiz").end();
	private static final PacketFormat RESPONSE = code(true).int32("vsiz").end();
	private Object key;
	private int vsiz;

	public Vsiz(Object key) {
		super((byte) 0x38, REQUEST, RESPONSE);
		this.key = key;
	}
	
	public Integer getReturnValue() {
		return isSuccess() ? vsiz : -1;
	}

	protected void pack(PacketContext context) {
		byte[] kbuf = keyTranscoder.encode(key);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
	}

	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			vsiz = (Integer)context.get("vsiz");
		}
	}
}
