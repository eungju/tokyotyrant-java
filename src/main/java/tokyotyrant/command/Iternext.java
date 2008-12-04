package tokyotyrant.command;

public class Iternext extends CommandSupport<Object> {
	private static final PacketFormat REQUEST = magic().end();
	private static final PacketFormat RESPONSE = code(true).int32("ksiz").bytes("kbuf", "ksiz").end();
	private Object key;
	
	public Iternext() {
		super((byte) 0x51, REQUEST, RESPONSE);
	}
	
	public Object getReturnValue() {
		return isSuccess() ? key : null;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			byte[] kbuf = (byte[])context.get("kbuf");
			key = keyTranscoder.decode(kbuf);
		}
	}
}
