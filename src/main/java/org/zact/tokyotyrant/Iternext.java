package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Iternext extends Command {
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
		return REQUEST.encode(REQUEST.context(magic));
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = RESPONSE.context();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		if (code == 0) {
			byte[] kbuf = (byte[])context.get("kbuf");
			key = transcoder.decode(kbuf);
		}
		return true;
	}
}
