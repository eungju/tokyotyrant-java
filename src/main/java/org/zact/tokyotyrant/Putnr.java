package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Putnr extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int32("vsiz"), bytes("kbuf", "ksiz"), bytes("vbuf", "vsiz"));
	private static final PacketSpec RESPONSE = packet();
	private Object key;
	private Object value;
	
	public Putnr(Object key, Object value) {
		super((byte) 0x18, REQUEST, RESPONSE);
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
	}
}
