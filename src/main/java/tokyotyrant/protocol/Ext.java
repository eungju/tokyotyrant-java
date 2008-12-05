package tokyotyrant.protocol;

public class Ext extends CommandSupport<Object> {
	private static final PacketFormat REQUEST = magic().int32("nsiz").int32("opts").int32("ksiz").int32("vsiz").bytes("nbuf", "nsiz").bytes("kbuf", "ksiz").bytes("vbuf", "vsiz").end();
	private static final PacketFormat RESPONSE = code(true).int32("rsiz").bytes("rbuf", "rsiz").end();
	private String name;
	private Object key;
	private Object value;
	private int opts;
	private Object result;
	
	public Ext(String name, Object key, Object value, int opts) {
		super((byte) 0x68, REQUEST, RESPONSE);
		this.name = name;
		this.key = key;
		this.value = value;
		this.opts = opts;
	}
	
	public Object getReturnValue() {
		return isSuccess() ? result : null;
	}
	
	protected void pack(PacketContext context) {
		byte[] nbuf = name.getBytes();
		byte[] kbuf = keyTranscoder.encode(key);
		byte[] vbuf = valueTranscoder.encode(value);
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
			result = valueTranscoder.decode(rbuf);
		}
	}
}
