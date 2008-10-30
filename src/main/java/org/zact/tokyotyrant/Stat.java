package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Stat extends Command {
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
		return REQUEST.encode(encodingContext(magic));
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
			sbuf = new String((byte[])context.get("sbuf"));
		}
		return done;
	}
}
