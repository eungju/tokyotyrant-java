package org.zact.tokyotyrant;

import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

public class Adddouble extends Command {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int64("integ"), int64("fract"), bytes("kbuf", "ksiz"));
	private static final PacketSpec RESPONSE = packet(code(true), int64("integ"), int64("fract"));
	private Object key;
	private double num;
	private double sum;
	
	public Adddouble(Object key, double num) {
		super((byte) 0x61);
		this.key = key;
		this.num = num;
	}
	
	public double getReturnValue() {
		return isSuccess() ? sum : Double.NaN;
	}
	
	private static final long TRILLION = (1000000L * 1000000L);
	
	public ByteBuffer encode() {
		PacketContext context = encodingContext(magic);
		byte[] kbuf = transcoder.encode(key);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
		
		long integ = (long)num;
		long fract = (long)((num - integ) * TRILLION);
		context.put("integ", integ);
		context.put("fract", fract);
		return REQUEST.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = PacketSpec.decodingContext();
		if (!RESPONSE.decode(context, in)) return false;
		code = (Byte)context.get("code");
		if (code == 0) {
			long integ = (Long)context.get("integ");
			long fract = (Long)context.get("fract");
			sum = integ + (fract / (double)TRILLION);
		}
		return true;
	}
}
