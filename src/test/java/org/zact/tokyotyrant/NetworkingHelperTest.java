package org.zact.tokyotyrant;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class NetworkingHelperTest {
	private NetworkingHelper dut;

	@Before public void beforeEach() {
		dut = new NetworkingHelper();
	}

	@Test public void accumulateBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putInt(1);
		ByteBuffer addition = ByteBuffer.allocate(4);
		addition.putInt(2).flip();
		ByteBuffer accumulatedBuffer = dut.accumulateBuffer(buffer, addition);
		accumulatedBuffer.flip();
		assertEquals(1, accumulatedBuffer.getInt());
		assertEquals(2, accumulatedBuffer.getInt());
		assertEquals(buffer.capacity(), accumulatedBuffer.capacity());
		assertSame(buffer, accumulatedBuffer);
	}

	@Test public void accumulateBufferShouldExpandWhenNecessary() {
		ByteBuffer buffer = ByteBuffer.allocate(7);
		buffer.putInt(1);
		ByteBuffer addition = ByteBuffer.allocate(4);
		addition.putInt(2).flip();
		ByteBuffer accumulatedBuffer = dut.accumulateBuffer(buffer, addition);
		accumulatedBuffer.flip();
		assertEquals(1, accumulatedBuffer.getInt());
		assertEquals(2, accumulatedBuffer.getInt());
		assertEquals(buffer.capacity() * 2, accumulatedBuffer.capacity());
		assertNotSame(buffer, accumulatedBuffer);
	}
}
