package tokyotyrant.command;

public class Addint extends CommandSupport<Integer> {
	private static final PacketFormat REQUEST = magic().int32("ksiz").int32("num").bytes("kbuf", "ksiz").end();
	private static final PacketFormat RESPONSE = code(true).int32("sum").end();
	private Object key;
	private int num;
	private int sum;
	
	public Addint(Object key, int num) {
		super((byte) 0x60, REQUEST, RESPONSE);
		this.key = key;
		this.num = num;
	}
	
	public Integer getReturnValue() {
		return isSuccess() ? sum : Integer.MIN_VALUE;
	}
	
	protected void pack(PacketContext context) {
		byte[] kbuf = keyTranscoder.encode(key);
		context.put("ksiz", kbuf.length);
		context.put("kbuf", kbuf);
		context.put("num", num);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		if (code == 0) {
			sum = (Integer)context.get("sum");
		}
	}
}
