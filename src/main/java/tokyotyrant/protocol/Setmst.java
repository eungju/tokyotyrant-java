package tokyotyrant.protocol;

public class Setmst extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = magic().int32("hsiz").int32("port").bytes("host", "hsiz").end();
	private static final PacketFormat RESPONSE = code(false).end();
	private final byte[] host;
	private final int port;
	
	public Setmst(String host, int port) {
		super((byte) 0x78, REQUEST, RESPONSE, null, null);
		this.host = host.getBytes();
		this.port = port;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		context.put("hsiz", host.length);
		context.put("host", host);
		context.put("port", port);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
