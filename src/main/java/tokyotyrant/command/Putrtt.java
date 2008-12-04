package tokyotyrant.command;

public class Putrtt extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = new PacketFormatBuilder().magic().int32("ksiz").int32("vsiz").int32("width").bytes("kbuf", "ksiz").bytes("vbuf", "vsiz").end();
	private static final PacketFormat RESPONSE = new PacketFormatBuilder().code(false).end();
	private Object key;
	private Object value;
	private int width;
	
	public Putrtt(Object key, Object value, int width) {
		super((byte) 0x13, REQUEST, RESPONSE);
		this.key = key;
		this.value = value;
		this.width = width;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] kbuf = keyTranscoder.encode(key);
		byte[] vbuf = valueTranscoder.encode(value);
		context.put("ksiz", kbuf.length);
		context.put("vsiz", vbuf.length);
		context.put("width", width);
		context.put("kbuf", kbuf);
		context.put("vbuf", vbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
