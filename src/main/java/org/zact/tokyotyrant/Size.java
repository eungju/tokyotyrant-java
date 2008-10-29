package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.CommandSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Size extends EasyCommand {
	private static final CommandSpec REQUEST = packet(magic());
	private static final CommandSpec RESPONSE = packet(code(true), field("size", Long.class, 8));
	private long size;
	             
	public Size() {
		super((byte) 0x81);
	}
	
	public long getReturnValue() {
		return size;
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(context());
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			size = (Long)context.get("size");
		}
		return done;
	}
}
