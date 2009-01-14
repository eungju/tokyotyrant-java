package tokyotyrant.helper;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class BufferHelperTest {
	@Test public void accumulateBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putInt(1);
		ByteBuffer addition = ByteBuffer.allocate(4);
		addition.putInt(2).flip();
		ByteBuffer accumulatedBuffer = BufferHelper.accumulateBuffer(buffer, addition);
		accumulatedBuffer.flip();
		assertEquals(1, accumulatedBuffer.getInt());
		assertEquals(2, accumulatedBuffer.getInt());
		assertEquals(buffer.capacity(), accumulatedBuffer.capacity());
		assertSame(buffer, accumulatedBuffer);
	}

	@Test public void accumulateBufferShouldExpandWhenNecessary() {
		ByteBuffer buffer = ByteBuffer.allocate(7);
		buffer.putInt(1);
		ByteBuffer addition = ByteBuffer.allocate(8);
		addition.putInt(2).flip();
		ByteBuffer accumulatedBuffer = BufferHelper.accumulateBuffer(buffer, addition);
		accumulatedBuffer.flip();
		assertEquals(1, accumulatedBuffer.getInt());
		assertEquals(2, accumulatedBuffer.getInt());
		assertEquals(buffer.capacity() * 2, accumulatedBuffer.capacity());
		assertNotSame(buffer, accumulatedBuffer);
	}
}
