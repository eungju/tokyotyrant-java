package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Putrtt extends Command {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int32("vsiz"), int32("width"), bytes("kbuf", "ksiz"), bytes("vbuf", "vsiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private Object key;
	private Object value;
	private int width;
	
	public Putrtt(Object key, Object value, int width) {
		super((byte) 0x13);
		this.key = key;
		this.value = value;
		this.width = width;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		PacketContext context = encodingContext(magic);
		byte[] kbuf = transcoder.encode(key);
		byte[] vbuf = transcoder.encode(value);
		context.put("ksiz", kbuf.length);
		context.put("vsiz", vbuf.length);
		context.put("width", width);
		context.put("kbuf", kbuf);
		context.put("vbuf", vbuf);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
		}
		return done;
	}
}
