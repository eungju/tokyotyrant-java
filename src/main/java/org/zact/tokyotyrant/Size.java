package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Size extends Command {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int64("size"));
	private long size;
	             
	public Size() {
		super((byte) 0x81);
	}
	
	public long getReturnValue() {
		return size;
	}
	
	public ByteBuffer encode() {
		return REQUEST.encode(REQUEST.context(magic));
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = RESPONSE.context();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
			size = (Long)context.get("size");
		}
		return done;
	}
}
