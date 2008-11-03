package org.zact.tokyotyrant;


public class Put extends PutCommandSupport {
	public Put(Object key, Object value) {
		super((byte) 0x10, key, value);
	}
}
