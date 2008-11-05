package tokyotyrant.command;

public class Putkeep extends PutCommandSupport {
	public Putkeep(Object key, Object value) {
		super((byte) 0x11, key, value);
	}
}
