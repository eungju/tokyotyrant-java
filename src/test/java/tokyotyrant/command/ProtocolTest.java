package tokyotyrant.command;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import tokyotyrant.ByteArrayTranscoder;
import tokyotyrant.Command;
import tokyotyrant.Transcoder;

public class ProtocolTest {
	private byte[] key = "key".getBytes();
	private byte[] value = "value".getBytes();
	
	private void setupTranscoders(Command<?> command) {
		Transcoder transcoder = new ByteArrayTranscoder();
		command.setKeyTranscoder(transcoder);
		command.setValueTranscoder(transcoder);
	}

	private void putFamily(PutCommandSupport dut, int commandId) {
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length + value.length)
			.put(new byte[] { (byte) 0xC8, (byte) commandId }).putInt(key.length).putInt(value.length).put(key).put(value);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertFalse(dut.decode(response));
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());

		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}
	
	@Test public void put() {
		putFamily(new Put(key, value), 0x10);
	}

	@Test public void putkeep() {
		putFamily(new Putkeep(key, value), 0x11);
	}

	@Test public void putcat() {
		putFamily(new Putcat(key, value), 0x12);
	}

	@Test public void putrtt() {
		int width = 1;
		Putrtt dut = new Putrtt(key, value, width);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + 4 + key.length + value.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x13 }).putInt(key.length).putInt(value.length).putInt(width).put(key).put(value);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test(expected=UnsupportedOperationException.class)
	public void putnr() {
		Putnr dut = new Putnr(key, value);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length + value.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x18 }).putInt(key.length).putInt(value.length).put(key).put(value);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertTrue(dut.decode(response));
		dut.getReturnValue();
	}

	@Test public void out() {
		Out dut = new Out(key);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x20 }).putInt(key.length).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void get() {
		Get dut = new Get(key);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x30 }).putInt(key.length).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + value.length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putInt(value.length).put(value).flip();
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[])dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void mget() {
		Mget dut = new Mget(new Object[] { key });
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x31 }).putInt(1).putInt(key.length).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + 4 + 4 + key.length + value.length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putInt(1).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putInt(key.length).putInt(value.length).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(key).put(value).flip();
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[]) dut.getReturnValue().values().iterator().next());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).putInt(0).flip();
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void vsiz() {
		Vsiz dut = new Vsiz(key);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x38 }).putInt(key.length).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + value.length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putInt(value.length).flip();
		assertTrue(dut.decode(response));
		assertEquals(value.length, (int)dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertEquals(-1, (int)dut.getReturnValue());
	}

	@Test public void iterinit() {
		Iterinit dut = new Iterinit();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x50 });
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void iternext() {
		Iternext dut = new Iternext();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x51 });
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + value.length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putInt(value.length).put(value).flip();
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[])dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void fwmkeys() {
		Fwmkeys dut = new Fwmkeys(key, Integer.MAX_VALUE);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x58 }).putInt(key.length).putInt(Integer.MAX_VALUE).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + 4 + key.length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putInt(1).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putInt(key.length).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(key).flip();
		assertTrue(dut.decode(response));
		assertEquals(1, dut.getReturnValue().size());
		assertArrayEquals(key, (byte[]) dut.getReturnValue().get(0));
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).putInt(0).flip();
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}
}
