package tokyotyrant.protocol;

public class PingCommand extends CommandSupport<Boolean> {
	private int ping;
	int pong;
	
	public PingCommand(int ping) {
		super((byte) 0xff, magic().int32("ping").end(), code(true).int32("pong").end());
		this.ping = ping;
	}
	
	public Boolean getReturnValue() {
		return true;
	}
	
	protected void pack(PacketContext context) {
		context.put("ping", ping);
	}
	
	protected void unpack(PacketContext context) {
		pong = (Integer)context.get("pong");
	}
}