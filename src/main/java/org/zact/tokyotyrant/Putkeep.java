package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Putkeep extends EasyCommand {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int32("vsiz"), bytes("kbuf", "ksiz"), bytes("vbuf", "vsiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private Object key;
	private Object value;
	
	public Putkeep(Object key, Object value) {
		super((byte) 0x11);
		this.key = key;
		this.value = value;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		Map<String, Object> context = context();
		byte[] kbuf = transcoder.encode(key);
		byte[] vbuf = transcoder.encode(value);
		context.put("ksiz", kbuf.length);
		context.put("vsiz", vbuf.length);
		context.put("kbuf", kbuf);
		context.put("vbuf", vbuf);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		boolean done = RESPONSE.decode(context, in);
		if (done) {
			code = (Byte)context.get("code");
		}
		return done;
	}
}
