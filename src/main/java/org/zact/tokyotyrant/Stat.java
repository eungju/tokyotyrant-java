package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.zact.tokyotyrant.CommandSpec.*;

public class Stat extends EasyCommand {
	private static final CommandSpec REQUEST = packet(magic());
	private static final CommandSpec RESPONSE = packet(code(true), field("ssize", Integer.class, 4), field("sbuf", String.class, "ssize"));
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
		Map<String, Object> decoded = new HashMap<String, Object>();
		boolean done = RESPONSE.decode(decoded, in);
		if (done) {
			sbuf = (String)decoded.get("sbuf");
		}
		return done;
	}
}
