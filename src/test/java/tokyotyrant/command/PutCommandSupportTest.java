package tokyotyrant.command;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import tokyotyrant.ByteArrayTranscoder;

public class PutCommandSupportTest {
	private PutCommandSupport dut;
	private byte[] key = "key".getBytes();
	private byte[] value = "value".getBytes();
	
	@Before public void beforeEach() {
		dut = new PutCommandSupport((byte) 0xff, key, value) {
		};
		dut.setKeyTranscoder(new ByteArrayTranscoder());
		dut.setValueTranscoder(new ByteArrayTranscoder());
	}
	
	@Test public void encodeShouldBeSuccefulAlways() {
		ByteBuffer expected = ByteBuffer.allocate(2 + 4 + 4 + key.length + value.length);
		expected.put((byte) 0xc8).put((byte) 0xff);
		expected.putInt(3).putInt(5);
		expected.put(key).put(value);
		expected.flip();
		ByteBuffer actual = dut.encode();
		assertEquals(expected, actual);
		assertArrayEquals(expected.array(), actual.array());
	}
	
	@Test public void decodeShouldBeCompletedWhenCodeIsGiven() {
		ByteBuffer input = ByteBuffer.allocate(1);
		input.put((byte) 0).flip();
		assertTrue(dut.decode(input));
	}

	@Test public void decodeShouldNotBeCompletedWhenCodeIsNotGiven() {
		ByteBuffer input = ByteBuffer.allocate(1);
		input.flip();
		assertFalse(dut.decode(input));
	}
	
	@Test public void returnValueShouldBeTrueWhenCodeIsZero() {
		ByteBuffer input = ByteBuffer.allocate(1);
		input.put((byte) 0).flip();
		dut.decode(input);
		assertTrue(dut.getReturnValue());
	}
	
	@Test public void returnValueShouldBeFalseWhenCodeIsNotZero() {
		ByteBuffer input = ByteBuffer.allocate(1);
		input.put((byte) 1).flip();
		dut.decode(input);
		assertFalse(dut.getReturnValue());
	}
}
