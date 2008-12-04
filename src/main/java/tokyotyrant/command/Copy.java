package tokyotyrant.command;

public class Copy extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = magic().int32("psiz").bytes("path", "psiz").end();
	private static final PacketFormat RESPONSE = code(false).end();
	private String path;
	
	public Copy(String path) {
		super((byte) 0x72, REQUEST, RESPONSE);
		this.path = path;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] pbuf = path.getBytes();
		context.put("psiz", pbuf.length);
		context.put("path", pbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
