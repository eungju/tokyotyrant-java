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
	}

	@Test public void putnr() {
		Putnr dut = new Putnr(key, value);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length + value.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x18 }).putInt(key.length).putInt(value.length).put(key).put(value);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1);
		response.flip();
		assertTrue(dut.decode(response));
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
		
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}
}
