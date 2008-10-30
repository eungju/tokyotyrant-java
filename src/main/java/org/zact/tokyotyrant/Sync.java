package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Sync extends Command {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true));
	             
	public Sync() {
		super((byte) 0x70);
	}
	
	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(encodingContext(magic));
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		return true;
	}
}
