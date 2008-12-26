package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class CommandSupportTest {
	private PingCommand dut;
	
	@Before public void beforeEach() {
		dut = new PingCommand(42);
	}
	
	@Test public void packBeforeEncodeToPacket() {
		ByteBuffer buffer = dut.encode();
		assertEquals((byte)0xc8, buffer.get());
		assertEquals((byte)0xff, buffer.get());
		assertEquals(42, buffer.getInt());
		assertFalse(buffer.hasRemaining());
	}

	@Test public void unpackWhenPacketDecodingIsCompleted() {
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		buffer.put((byte)0).putInt(43);
		buffer.flip();
		assertTrue(dut.decode(buffer));
		assertEquals(43, dut.pong);
	}

	@Test public void doNotUnpackWhenPacketDecodingIsNotCompleted() {
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		buffer.put((byte)0);
		buffer.flip();
		assertFalse(dut.decode(buffer));
		assertEquals(0, dut.pong);
	}
	
	@Test public void encodingContextShouldContainMagicByDefault() {
		assertArrayEquals(new byte[] {(byte) 0xc8, (byte) 0xff}, (byte[])dut.encodingContext().get("magic"));
	}
}
