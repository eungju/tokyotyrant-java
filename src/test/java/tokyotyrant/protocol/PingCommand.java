package tokyotyrant.protocol;

public class PingCommand extends BinaryCommandSupport<Boolean> {
	private int ping;
	int pong;
	
	public PingCommand(int ping) {
		super((byte) 0xff, magic().int32("ping").end(), code(true).int32("pong").end(), null, null);
		this.ping = ping;
	}
	
	public Boolean getReturnValue() {
		return true;
	}
	
	protected void pack(PacketContext context) {
		context.put("ping", ping);
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte) context.get("code");
		if (code == ESUCCESS) {
			pong = (Integer) context.get("pong");
		}
	}
}