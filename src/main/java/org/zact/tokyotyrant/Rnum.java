package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Rnum extends Command {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int64("rnum"));
	private long rnum;

	public Rnum() {
		super((byte) 0x80);
	}
	
	public long getReturnValue() {
		return rnum;
	}

	public ByteBuffer encode() {
		return REQUEST.encode(encodingContext(magic));
	}

	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
			rnum = (Long)context.get("rnum");
		}
		return done;
	}
}
