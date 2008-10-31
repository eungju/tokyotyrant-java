package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Iterinit extends Command {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false));
	             
	public Iterinit() {
		super((byte) 0x50);
	}
	
	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(REQUEST.context(magic));
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = RESPONSE.context();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		return true;
	}
}
