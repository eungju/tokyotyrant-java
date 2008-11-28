package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Size extends CommandSupport<Long> {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int64("size"));
	private long size;
	             
	public Size() {
		super((byte) 0x81, REQUEST, RESPONSE);
	}
	
	public Long getReturnValue() {
		return size;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		size = (Long)context.get("size");
	}
}
