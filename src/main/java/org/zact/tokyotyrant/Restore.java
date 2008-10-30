package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Restore extends Command {
	private static final PacketSpec REQUEST = packet(magic(), int32("psiz"), int64("ts"), bytes("path", "psiz"));
	private static final PacketSpec RESPONSE = packet(code(false));
	private String path;
	private long ts;
	
	public Restore(String path, long ts) {
		super((byte) 0x73);
		this.path = path;
		this.ts = ts;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		PacketContext context = encodingContext(magic);
		byte[] pbuf = path.getBytes();
		context.put("psiz", pbuf.length);
		context.put("ts", ts);
		context.put("path", pbuf);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		return true;
	}
}
