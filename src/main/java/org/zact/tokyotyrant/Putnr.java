package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

public class Putnr extends PutCommandSupport {
	public Putnr(Object key, Object value) {
		super((byte) 0x18, key, value);
	}
	
	public boolean decode(ByteBuffer in) {
		return true;
	}
}
