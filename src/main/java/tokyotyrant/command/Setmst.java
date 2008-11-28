package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Setmst extends CommandSupport<Boolean> {
	private static final PacketSpec REQUEST = packet(magic(), int32("hsiz"), int32("port"), bytes("host", "hsiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private String host;
	private int port;
	
	public Setmst(String host, int port) {
		super((byte) 0x78, REQUEST, RESPONSE);
		this.host = host;
		this.port = port;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] hbuf = host.getBytes();
		context.put("hsiz", hbuf.length);
		context.put("host", hbuf);
		context.put("port", port);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
