package org.zact.tokyotyrant;

import static org.junit.Assert.*;
import static org.zact.tokyotyrant.PacketSpec.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class PacketSpecTest {
	@Test public void shouldEncodeStaticSizeField() {
		PacketContext context = new PacketContext();
		context.put("magic", new byte[] {(byte) 0xC8, (byte) 0x80});
		ByteBuffer actual = packet(bytes("magic", 2)).encode(context);
		assertArrayEquals(new byte[] {(byte) 0xC8, (byte) 0x80}, actual.array());
	}
	
	@Test public void shouldEncodeVariableSizeField() {
		PacketContext context = new PacketContext();
		context.put("ksiz", 3);
		context.put("kbuf", new byte[] {1, 2, 3});
		ByteBuffer actual = packet(int32("ksiz"), bytes("kbuf", "ksiz")).encode(context);
		assertArrayEquals(ByteBuffer.allocate(4 + 3).putInt(3).put(new byte[] {1, 2, 3}).array(), actual.array());
	}
	
	@Test public void shouldDecodeStaticSizeField() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		PacketContext context = new PacketContext();
		assertTrue(packet(int8("code")).decode(context, in));
		assertEquals((byte)1, context.get("code"));
	}
	
	@Test public void shouldDecodeVariableSizeField() {
		ByteBuffer in = ByteBuffer.allocate(4 + 3);
		in.putInt(3).put(new byte[] {1, 2, 3}).flip();
		PacketContext context = new PacketContext();
		assertTrue(packet(int32("vsiz"), bytes("vbuf", "vsiz")).decode(context, in));
		assertEquals(3, context.get("vsiz"));
		assertArrayEquals(new byte[] {1, 2, 3}, (byte[])context.get("vbuf"));
	}

	@Test public void shouldStopToDecodeAfterCodeWhenError() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		PacketContext context = new PacketContext();
		assertTrue(packet(code(true), int32("vsiz")).decode(context, in));
	}

	@Test public void shouldNotStopDecodeAfterCodeWhenSuccess() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 0).flip();
		PacketContext context = new PacketContext();
		assertFalse(packet(code(true), int32("vsiz")).decode(context, in));
	}

	@Test public void shouldNotStopDecodeAfterCode() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		PacketContext context = new PacketContext();
		assertFalse(packet(code(false), int32("vsiz")).decode(context, in));
	}
}
