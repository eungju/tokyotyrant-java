package tokyotyrant.command;

public class Rnum extends CommandSupport<Long> {
	private static final PacketFormat REQUEST = new PacketFormatBuilder().magic().end();
	private static final PacketFormat RESPONSE = new PacketFormatBuilder().code(false).int64("rnum").end();
	private long rnum;

	public Rnum() {
		super((byte) 0x80, REQUEST, RESPONSE);
	}
	
	public Long getReturnValue() {
		return rnum;
	}

	protected void pack(PacketContext context) {
	}

	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		rnum = (Long)context.get("rnum");
	}
}
