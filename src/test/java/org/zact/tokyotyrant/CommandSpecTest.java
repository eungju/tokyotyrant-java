package org.zact.tokyotyrant;

import static org.junit.Assert.*;
import static org.zact.tokyotyrant.CommandSpec.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CommandSpecTest {
	@Test public void shouldEncodeStaticSizeField() {
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("magic", new byte[] {(byte) 0xC8, (byte) 0x80});
		ByteBuffer actual = packet(field("magic", byte[].class, 2)).encode(context);
		assertArrayEquals(new byte[] {(byte) 0xC8, (byte) 0x80}, actual.array());
	}
	
	@Test public void shouldEncodeVariableSizeField() {
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("ksiz", 3);
		context.put("kbuf", new byte[] {1, 2, 3});
		ByteBuffer actual = packet(field("ksiz", Integer.class, 4), field("kbuf", byte[].class, "ksiz")).encode(context);
		assertArrayEquals(ByteBuffer.allocate(4 + 3).putInt(3).put(new byte[] {1, 2, 3}).array(), actual.array());
	}
	
	@Test public void shouldDecodeStaticSizeField() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		Map<String, Object> context = new HashMap<String, Object>();
		assertTrue(packet(field("code", Byte.class, 1)).decode(context, in));
		assertEquals((byte)1, context.get("code"));
	}
	
	@Test public void shouldDecodeVariableSizeField() {
		ByteBuffer in = ByteBuffer.allocate(4 + 3);
		in.putInt(3).put(new byte[] {1, 2, 3}).flip();
		Map<String, Object> context = new HashMap<String, Object>();
		assertTrue(packet(field("vsiz", Integer.class, 4), field("vbuf", byte[].class, "vsiz")).decode(context, in));
		assertEquals(3, context.get("vsiz"));
		assertArrayEquals(new byte[] {1, 2, 3}, (byte[])context.get("vbuf"));
	}

	@Test public void shouldStopToDecodeAfterCodeWhenFailed() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		Map<String, Object> context = new HashMap<String, Object>();
		assertTrue(packet(code(false)).decode(context, in));
		assertEquals((byte)1, context.get("code"));
	}

	@Test public void shouldNotStopDecodeAfterCodeWhenSuccess() {
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 0).flip();
		Map<String, Object> context = new HashMap<String, Object>();
		assertTrue(packet(code(false)).decode(context, in));
		assertEquals((byte)0, context.get("code"));
	}
}
