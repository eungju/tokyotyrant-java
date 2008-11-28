package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Copy extends CommandSupport<Boolean> {
	private static final PacketSpec REQUEST = packet(magic(), int32("psiz"), bytes("path", "psiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private String path;
	
	public Copy(String path) {
		super((byte) 0x72, REQUEST, RESPONSE);
		this.path = path;
	}

	public Boolean getReturnValue() {
		return isSuccess();
	}
	
	protected void pack(PacketContext context) {
		byte[] pbuf = path.getBytes();
		context.put("psiz", pbuf.length);
		context.put("path", pbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
	}
}
