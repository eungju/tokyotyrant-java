package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Putkeep extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int32("vsiz"), bytes("kbuf", "ksiz"), bytes("vbuf", "vsiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private Object key;
	private Object value;
	
	public Putkeep(Object key, Object value) {
		super((byte) 0x11, REQUEST, RESPONSE);
		this.key = key;
		this.value = value;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] kbuf = transcoder.encode(key);
		byte[] vbuf = transcoder.encode(value);
		context.put("ksiz", kbuf.length);
		context.put("vsiz", vbuf.length);
		context.put("kbuf", kbuf);
		context.put("vbuf", vbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
