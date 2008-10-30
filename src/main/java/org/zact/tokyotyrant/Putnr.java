package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.CommandSpec.*;

import java.nio.ByteBuffer;
import java.util.Map;

public class Putnr extends EasyCommand {
	private static final CommandSpec REQUEST = packet(
			magic(),
			field("ksiz", Integer.class, 4), field("vsiz", Integer.class, 4),
			field("kbuf", byte[].class, "ksiz"), field("vbuf", byte[].class, "vsiz")
			);
	private static final CommandSpec RESPONSE = packet();
	private Object key;
	private Object value;
	
	public Putnr(Object key, Object value) {
		super((byte) 0x18);
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
		return RESPONSE.decode(null, in);
	}
}
