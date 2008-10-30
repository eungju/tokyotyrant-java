package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Stat extends EasyCommand {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int32("ssiz"), bytes("sbuf", "ssiz"));
	private String sbuf;
	             
	public Stat() {
		super((byte) 0x88);
	}
	
	public String getReturnValue() {
		return sbuf;
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(context());
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
			sbuf = new String((byte[])context.get("sbuf"));
		}
		return done;
	}
}
