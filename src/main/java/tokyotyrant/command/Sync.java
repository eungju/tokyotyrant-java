package tokyotyrant.command;

public class Sync extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = new PacketFormatBuilder().magic().end();
	private static final PacketFormat RESPONSE = new PacketFormatBuilder().code(true).end();
	             
	public Sync() {
		super((byte) 0x70, REQUEST, RESPONSE);
	}
	
	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
