package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Vanish extends Command {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true));
	             
	public Vanish() {
		super((byte) 0x71);
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
