package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Vsiz extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), bytes("kbuf", "ksiz"));
	private static final PacketSpec RESPONSE = packet(code(true), int32("vsiz"));
	private Object key;
	private int vsiz;

	public Vsiz(Object key) {
		super((byte) 0x38, REQUEST, RESPONSE);
		this.key = key;
	}
	
	public int getReturnValue() {
		return isSuccess() ? vsiz : -1;
	}

	protected void pack(PacketContext context) {
		byte[] kbuf = transcoder.encode(key);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
	}

	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			vsiz = (Integer)context.get("vsiz");
		}
	}
}
