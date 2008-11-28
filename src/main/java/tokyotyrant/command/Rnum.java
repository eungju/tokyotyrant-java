package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Rnum extends CommandSupport<Long> {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(false), int64("rnum"));
	private long rnum;

	public Rnum() {
		super((byte) 0x80, REQUEST, RESPONSE);
	}
	
	public Long getReturnValue() {
		return rnum;
	}

	protected void pack(PacketContext context) {
	}

	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		rnum = (Long)context.get("rnum");
	}
}
