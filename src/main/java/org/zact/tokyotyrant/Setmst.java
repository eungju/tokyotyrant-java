package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Setmst extends Command {
	private static final PacketSpec REQUEST = packet(magic(), int32("hsiz"), int32("port"), bytes("host", "hsiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private String host;
	private int port;
	
	public Setmst(String host, int port) {
		super((byte) 0x78);
		this.host = host;
		this.port = port;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		PacketContext context = REQUEST.context(magic);
		byte[] hbuf = host.getBytes();
		context.put("hsiz", hbuf.length);
		context.put("host", hbuf);
		context.put("port", port);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = RESPONSE.context();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
		}
		return done;
	}
}
