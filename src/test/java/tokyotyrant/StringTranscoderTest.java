package tokyotyrant;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class StringTranscoderTest {
	private StringTranscoder dut;

	@Before public void beforeEach() {
		dut = new StringTranscoder();
	}
	
	@Test public void encodeString() {
		assertArrayEquals("value".getBytes(), dut.encode("value"));
	}

	@Test public void encodeInt() {
		assertArrayEquals("42".getBytes(), dut.encode(42));
	}

	@Test public void decode() {
		assertEquals("value", (String)dut.decode("value".getBytes()));
	}
}
