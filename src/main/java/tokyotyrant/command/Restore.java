package tokyotyrant.command;

public class Restore extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = magic().int32("psiz").int64("ts").bytes("path", "psiz").end();
	private static final PacketFormat RESPONSE = code(false).end();
	private String path;
	private long ts;
	
	public Restore(String path, long ts) {
		super((byte) 0x73, REQUEST, RESPONSE);
		this.path = path;
		this.ts = ts;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] pbuf = path.getBytes();
		context.put("psiz", pbuf.length);
		context.put("ts", ts);
		context.put("path", pbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
