package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Iternext extends EasyCommand {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true), int32("ksiz"), bytes("kbuf", "ksiz"));
	private Object key;
	
	public Iternext() {
		super((byte) 0x51);
	}
	
	public Object getReturnValue() {
		return isSuccess() ? key : null;
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(context());
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		if (code == 0) {
			byte[] kbuf = (byte[])context.get("kbuf");
			key = transcoder.decode(kbuf);
		}
		return true;
	}
}
