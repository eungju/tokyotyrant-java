package tokyotyrant.transcoder;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;

public class SerializableTranscoderTest {
	private SerializableTranscoder dut;

	static class DummyObject implements Serializable {
		private static final long serialVersionUID = -8120203185641868658L;
		int attr;
		public DummyObject(int attr) {
			this.attr = attr;
		}
	}
	
	@Before public void beforEach() {
		dut = new SerializableTranscoder();
	}
	
	@Test public void encode() {
		assertNotNull(dut.encode(new DummyObject(1)));
	}

	@Test public void decode() {
		assertEquals(1, ((DummyObject)dut.decode(dut.encode(new DummyObject(1)))).attr);
	}
}
