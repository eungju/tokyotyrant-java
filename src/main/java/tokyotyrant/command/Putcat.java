package tokyotyrant.command;

public class Putcat extends PutCommandSupport {
	public Putcat(Object key, Object value) {
		super((byte) 0x12, key, value);
	}
}