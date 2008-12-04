package tokyotyrant.command;

public class Vanish extends CommandSupport<Boolean> {
	private static final PacketFormat REQUEST = new PacketFormatBuilder().magic().end();
	private static final PacketFormat RESPONSE = new PacketFormatBuilder().code(true).end();
	             
	public Vanish() {
		super((byte) 0x71, REQUEST, RESPONSE);
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
