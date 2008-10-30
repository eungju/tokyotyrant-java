package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Iterinit extends EasyCommand {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false));
	             
	public Iterinit() {
		super((byte) 0x50);
	}
	
	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(context());
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		return true;
	}
}
