package tokyotyrant.transcoder;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteTranscoderTest extends TranscoderTest {
	@Before public void beforeEach() {
		dut = new ByteTranscoder();
	}
	
	@Test public void encode() {
		assertArrayEquals(new byte[] {0x12}, dut.encode(0x12));
	}
	
	@Test public void decode() {
		assertEquals(0x12, dut.decode(new byte[] {0x12}));
	}

	@Test(expected=IllegalArgumentException.class)
	public void shouldNotDecodeInvalid() {
		dut.decode(new byte[] {0x12, 0x34});
	}
}
