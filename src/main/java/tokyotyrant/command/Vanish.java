package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Vanish extends CommandSupport<Boolean> {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true));
	             
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
