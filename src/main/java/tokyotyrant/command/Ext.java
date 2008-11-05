package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Ext extends CommandSupport {
	private static final PacketSpec REQUEST = packet(magic(), int32("nsiz"), int32("opts"), int32("ksiz"), int32("vsiz"), bytes("nbuf", "nsiz"), bytes("kbuf", "ksiz"), bytes("vbuf", "vsiz"));
	private static final PacketSpec RESPONSE = packet(code(true), int32("rsiz"), bytes("rbuf", "rsiz"));
	private String name;
	private int opts;
	private Object key;
	private Object value;
	private Object result;
	
	public Ext(String name, int opts, Object key, Object value) {
		super((byte) 0x68, REQUEST, RESPONSE);
		this.name = name;
		this.opts = opts;
		this.key = key;
		this.value = value;
	}
	
	public Object getReturnValue() {
		return isSuccess() ? result : null;
	}
	
	protected void pack(PacketContext context) {
		byte[] nbuf = name.getBytes();
		byte[] kbuf = transcoder.encode(key);
		byte[] vbuf = transcoder.encode(value);
		context.put("nsiz", nbuf.length);
		context.put("opts", opts);
		context.put("ksiz", kbuf.length);
		context.put("vsiz", vbuf.length);
		context.put("nbuf", nbuf);
		context.put("kbuf", kbuf);
		context.put("vbuf", vbuf);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			byte[] rbuf = (byte[])context.get("rbuf");
			result = transcoder.decode(rbuf);
		}
	}
}
