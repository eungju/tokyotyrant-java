package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Sync extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true));
	             
	public Sync() {
		super((byte) 0x70, REQUEST, RESPONSE);
	}
	
	public boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
