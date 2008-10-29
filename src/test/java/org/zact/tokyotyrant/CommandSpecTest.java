package org.zact.tokyotyrant;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CommandSpecTest {
	@Test public void shouldAcceptOneFieldSpec() {
		CommandSpec spec = new CommandSpec("[magic:2]");
		assertEquals(Arrays.asList(CommandSpec.field("magic", 2)), spec.fieldSpecs);
	}

	@Test public void shouldAcceptMultiFieldSpec() {
		CommandSpec spec = new CommandSpec("[magic:2][monkey:4]");
		assertEquals(Arrays.asList(CommandSpec.field("magic", 2), CommandSpec.field("monkey", 4)), spec.fieldSpecs);
	}
	
	@Test public void shouldEncodeStaticSizeField() {
		CommandSpec spec = new CommandSpec("[magic:2]");
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("magic", new byte[] {(byte) 0xC8, (byte) 0x80});
		ByteBuffer actual = spec.encode(context);
		assertArrayEquals(new byte[] {(byte) 0xC8, (byte) 0x80}, actual.array());
	}
	
	@Test public void shouldEncodeVariableSizeField() {
		CommandSpec spec = new CommandSpec("[ksiz:4][kbuf:ksiz]");
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("ksiz", 3);
		context.put("kbuf", new byte[] {1, 2, 3});
		ByteBuffer actual = spec.encode(context);
		assertArrayEquals(new byte[] {0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03}, actual.array());
	}
	
	@Test public void shouldDecodeStaticSizeField() {
		CommandSpec spec = new CommandSpec("[code:1]");
		ByteBuffer in = ByteBuffer.allocate(1);
		in.put((byte) 1).flip();
		Map<String, Object> decoded = new HashMap<String, Object>();
		assertTrue(spec.decode(decoded, in));
		assertEquals((byte)1, decoded.get("code"));
	}
	
	@Test public void shouldDecodeVariableSizeField() {
		CommandSpec spec = new CommandSpec("[vsiz:4][vbuf:vsiz]");
		ByteBuffer in = ByteBuffer.allocate(4 + 3);
		in.putInt(3).put(new byte[] {1, 2, 3}).flip();
		Map<String, Object> decoded = new HashMap<String, Object>();
		assertTrue(spec.decode(decoded, in));
		assertEquals(3, decoded.get("vsiz"));
		assertArrayEquals(new byte[] {1, 2, 3}, (byte[])decoded.get("vbuf"));
	}
}
