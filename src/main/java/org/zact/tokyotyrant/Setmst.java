package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.CommandSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Setmst extends EasyCommand {
	private static final CommandSpec REQUEST = packet(magic(), field("hsize", Integer.class, 4), field("port", Integer.class, 4), field("host", String.class, "hsize"));
	private static final CommandSpec RESPONSE = packet(code(true));
	private String host;
	private int port;
	
	public Setmst(String host, int port) {
		super((byte) 0x78);
		this.host = host;
		this.port = port;
	}

	public boolean getReturnValue() {
		return isSuccess();
	}
	
	public ByteBuffer encode() {
		Map<String, Object> context = context();
		context.put("hsize", host.getBytes().length);
		context.put("host", host);
		context.put("port", port);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		Map<String, Object> context = new HashMap<String, Object>();
		return RESPONSE.decode(context, in);
	}
}
