package tokyotyrant.helper;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class BufferHelperTest {
	@Test public void expand() {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.put(new byte[] { 1, 2, 3 });
		ByteBuffer expanded = BufferHelper.expand(buffer);
		assertEquals(buffer.capacity() * 2, expanded.capacity());
		assertEquals(buffer.position(), expanded.position());
		assertEquals(expanded.capacity(), expanded.limit());
		assertArrayEquals(new byte[] { 1, 2, 3, 0, 0, 0, 0, 0 }, expanded.array());
	}
}
