package tokyotyrant.transcoder;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SerializableTranscoderTest extends TranscoderTest {
	static class SerializableObject implements Serializable {
		private static final long serialVersionUID = -8120203185641868658L;
		public boolean equals(Object o) {
			return EqualsBuilder.reflectionEquals(this, o);
		}
		public int hashCode() {
			return HashCodeBuilder.reflectionHashCode(this);
		}
	}
	
	@Before public void beforEach() {
		dut = new SerializableTranscoder();
	}
	
	@Test public void encode() {
		assertNotNull(dut.encode(new SerializableObject()));
	}

	@Test public void decode() {
		assertEquals(new SerializableObject(), ((SerializableObject)dut.decode(dut.encode(new SerializableObject()))));
	}
}
