package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

public class Size extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int64("size"));
	private long size;
	             
	public Size() {
		super((byte) 0x81, REQUEST, RESPONSE);
	}
	
	public long getReturnValue() {
		return size;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		size = (Long)context.get("size");
	}
}
