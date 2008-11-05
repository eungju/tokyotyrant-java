package tokyotyrant.command;

import static org.junit.Assert.*;
import static tokyotyrant.command.PacketSpec.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import tokyotyrant.command.PacketSpec.FieldSpec;

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
	
	@Test public void encodeAndDecodeByteArrays() {
		String fieldName = "field";
		PacketContext encodingContext = new PacketContext();
		PacketContext decodingContext = new PacketContext();
		PacketSpec spec = packet(bytes(fieldName, 1));
		encodingContext.put(fieldName, new byte[] {42});
		ByteBuffer buffer = spec.encode(encodingContext);
		assertArrayEquals(new byte[] {42}, (byte[])encodingContext.get(fieldName));
		spec.decode(decodingContext, buffer);
		assertArrayEquals((byte[])decodingContext.get(fieldName), (byte[])encodingContext.get(fieldName));
	}

	@Test public void encodeAndDecodeInt32s() {
		String fieldName = "field";
		PacketContext encodingContext = new PacketContext();
		PacketContext decodingContext = new PacketContext();
		PacketSpec spec = packet(int32(fieldName));
		encodingContext.put(fieldName, 42);
		ByteBuffer buffer = spec.encode(encodingContext);
		assertEquals(42, encodingContext.get(fieldName));
		spec.decode(decodingContext, buffer);
		assertEquals(decodingContext.get(fieldName), encodingContext.get(fieldName));
	}

	@Test public void encodeAndDecodeInt8s() {
		String fieldName = "field";
		PacketContext encodingContext = new PacketContext();
		PacketContext decodingContext = new PacketContext();
		PacketSpec spec = packet(int8(fieldName));
		encodingContext.put(fieldName, (byte) 42);
		ByteBuffer buffer = spec.encode(encodingContext);
		assertTrue(encodingContext.get(fieldName) instanceof Byte);
		assertEquals((byte)42, encodingContext.get(fieldName));
		spec.decode(decodingContext, buffer);
		assertEquals((Byte)decodingContext.get(fieldName), encodingContext.get(fieldName));
	}

	@Test public void encodeAndDecodeInt64s() {
		String fieldName = "field";
		PacketContext encodingContext = new PacketContext();
		PacketContext decodingContext = new PacketContext();
		PacketSpec spec = packet(int64(fieldName));
		encodingContext.put(fieldName, 42L);
		ByteBuffer buffer = spec.encode(encodingContext);
		assertTrue(encodingContext.get(fieldName) instanceof Long);
		assertEquals(42L, encodingContext.get(fieldName));
		spec.decode(decodingContext, buffer);
		assertEquals((Long)decodingContext.get(fieldName), encodingContext.get(fieldName));
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void encodeUnsupportedTypeDouble() {
		String fieldName = "field";
		PacketSpec spec = packet(new FieldSpec(fieldName, Double.class, 8));
		PacketContext encodingContext = new PacketContext();
		encodingContext.put(fieldName, 42L);
		spec.encode(encodingContext);
	}

	@Test(expected=UnsupportedOperationException.class)
	public void decodeUnsupportedTypeDouble() {
		String fieldName = "field";
		PacketSpec spec = packet(new FieldSpec(fieldName, Double.class, 8));
		PacketContext decodingContext = new PacketContext();
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putDouble(42).flip();
		spec.decode(decodingContext, buffer);
	}
}
