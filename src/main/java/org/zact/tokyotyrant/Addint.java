package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Addint extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int32("num"), bytes("kbuf", "ksiz"));
	private static final PacketSpec RESPONSE = packet(code(true), int32("sum"));
	private Object key;
	private int num;
	private int sum;
	
	public Addint(Object key, int num) {
		super((byte) 0x60, REQUEST, RESPONSE);
		this.key = key;
		this.num = num;
	}
	
	public int getReturnValue() {
		return isSuccess() ? sum : Integer.MIN_VALUE;
	}
	
	protected void pack(PacketContext context) {
		byte[] kbuf = transcoder.encode(key);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
		context.put("num", num);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			sum = (Integer)context.get("sum");
		}
	}
}
