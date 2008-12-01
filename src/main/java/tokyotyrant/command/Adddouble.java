package tokyotyrant.command;

import static tokyotyrant.command.PacketSpec.*;

public class Adddouble extends CommandSupport<Double> {
	private static final PacketSpec REQUEST = packet(magic(), int32("ksiz"), int64("integ"), int64("fract"), bytes("kbuf", "ksiz"));
	private static final PacketSpec RESPONSE = packet(code(true), int64("integ"), int64("fract"));
	private Object key;
	private double num;
	private double sum;
	
	public Adddouble(Object key, double num) {
		super((byte) 0x61, REQUEST, RESPONSE);
		this.key = key;
		this.num = num;
	}
	
	public Double getReturnValue() {
		return isSuccess() ? sum : Double.NaN;
	}
	
	private static final long TRILLION = (1000000L * 1000000L);
	
	protected void pack(PacketContext context) {
		byte[] kbuf = keyTranscoder.encode(key);
		long integ = (long)num;
		long fract = (long)((num - integ) * TRILLION);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
		context.put("integ", integ);
		context.put("fract", fract);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			long integ = (Long)context.get("integ");
			long fract = (Long)context.get("fract");
			sum = integ + (fract / (double)TRILLION);
		}
	}
}
