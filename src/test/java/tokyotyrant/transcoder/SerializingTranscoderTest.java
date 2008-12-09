package tokyotyrant.transcoder;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

public class SerializingTranscoderTest extends TranscoderTest {
	@Before public void beforeEach() {
		dut = new SerializingTranscoder();
	}
	
	@Test public void transcodeString() {
		String decoded = "foo";
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_STRING, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeBoolean() {
		boolean decoded = true;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_BOOLEAN, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeInteger() {
		int decoded = 42;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_INTEGER, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeLong() {
		long decoded = 42L;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_LONG, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeDate() {
		Date decoded = new Date();
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_DATE, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeByte() {
		byte decoded = 42;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_BYTE, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeFloat() {
		float decoded = 42F;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_FLOAT, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeDouble() {
		double decoded = 42D;
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_DOUBLE, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}

	@Test public void transcodeByteArray() {
		byte[] decoded = new byte[] { 42 };
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_BYTEARRAY, encoded[0]);
		assertArrayEquals(decoded, (byte[])dut.decode(encoded));
	}

	@Test public void transcodeSerializable() {
		Serializable decoded = new SerializableTranscoderTest.SerializableObject();
		byte[] encoded = dut.encode(decoded);
		assertEquals(SerializingTranscoder.TYPE_SERIALIZABLE, encoded[0]);
		assertEquals(decoded, dut.decode(encoded));
	}
}
