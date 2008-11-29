package tokyotyrant;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteArrayTranscoderTest {
	private ByteArrayTranscoder dut;

	@Before public void beforeEach() {
		dut = new ByteArrayTranscoder();
	}
	
	@Test public void encode() {
		byte[] value = new byte[] {42};
		assertArrayEquals(value, dut.encode(value));
	}

	@Test public void decode() {
		byte[] value = new byte[] {42};
		assertArrayEquals(value, (byte[])dut.decode(value));
	}
}