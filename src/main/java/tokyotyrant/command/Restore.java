package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Restore extends CommandSupport<Boolean> {
	private static final PacketSpec REQUEST = packet(magic(), int32("psiz"), int64("ts"), bytes("path", "psiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private String path;
	private long ts;
	
	public Restore(String path, long ts) {
		super((byte) 0x73, REQUEST, RESPONSE);
		this.path = path;
		this.ts = ts;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] pbuf = path.getBytes();
		context.put("psiz", pbuf.length);
		context.put("ts", ts);
		context.put("path", pbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
