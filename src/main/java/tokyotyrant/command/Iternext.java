package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Iternext extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic());
	private static final PacketSpec RESPONSE = packet(code(true), int32("ksiz"), bytes("kbuf", "ksiz"));
	private Object key;
	
	public Iternext() {
		super((byte) 0x51, REQUEST, RESPONSE);
	}
	
	public Object getReturnValue() {
		return isSuccess() ? key : null;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			byte[] kbuf = (byte[])context.get("kbuf");
			key = transcoder.decode(kbuf);
		}
	}
}
