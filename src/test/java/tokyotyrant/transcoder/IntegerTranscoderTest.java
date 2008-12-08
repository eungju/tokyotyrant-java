package tokyotyrant.transcoder;

import static org.junit.Assert.*;

import java.nio.ByteOrder;

import org.junit.Before;
import org.junit.Test;

public class IntegerTranscoderTest {
	private IntegerTranscoder dut;

	@Before public void beforeEach() {
		dut = new IntegerTranscoder(ByteOrder.BIG_ENDIAN);
	}
	
	@Test public void encode() {
		assertArrayEquals(new byte[] {0x12, 0x34, 0x56, 0x78}, dut.encode(0x12345678));
	}
	
	@Test public void decode() {
		assertEquals(0x12345678, dut.decode(new byte[] {0x12, 0x34, 0x56, 0x78}));
	}

	@Test(expected=IllegalArgumentException.class)
	public void failToDecodeWhenNotAnInteger() {
		dut.decode(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x90});
	}
}
