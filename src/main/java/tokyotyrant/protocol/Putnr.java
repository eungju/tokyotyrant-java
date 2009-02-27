package tokyotyrant.protocol;

import org.jboss.netty.buffer.ChannelBuffer;

public class Putnr extends PutCommandSupport {
	public Putnr(Object key, Object value) {
		super((byte) 0x18, key, value);
	}
	
	public Boolean getReturnValue() {
		throw new UnsupportedOperationException("Putnr has no return value");
	}

	public boolean decode(ChannelBuffer in) {
		return true;
	}
}
