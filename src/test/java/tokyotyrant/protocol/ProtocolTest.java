package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.Transcoder;

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
		
		ChannelBuffer expected = ChannelBuffers.buffer(2 + 4 + 4 + key.length + value.length);
		expected.writeBytes(new byte[] { (byte) 0xC8, (byte) commandId });
		expected.writeInt(key.length);
		expected.writeInt(value.length);
		expected.writeBytes(key);
		expected.writeBytes(value);
		ChannelBuffer actual = ChannelBuffers.buffer(expected.capacity());
		dut.encode(actual);
		assertEquals(expected, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));

		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());

		response.clear();
		response.writeByte(Command.EUNKNOWN);
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

	@Test public void putshl() {
		int width = 1;
		Putshl dut = new Putshl(key, value, width);
		setupTranscoders(dut);
		
		ChannelBuffer expected = ChannelBuffers.buffer(2 + 4 + 4 + 4 + key.length + value.length);
		expected.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x13 });
		expected.writeInt(key.length);
		expected.writeInt(value.length);
		expected.writeInt(width);
		expected.writeBytes(key);
		expected.writeBytes(value);
		ChannelBuffer actual = ChannelBuffers.buffer(expected.capacity());
		dut.encode(actual);
		assertEquals(expected, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test(expected=UnsupportedOperationException.class)
	public void putnr() {
		Putnr dut = new Putnr(key, value);
		setupTranscoders(dut);
		
		ChannelBuffer expected = ChannelBuffers.buffer(2 + 4 + 4 + key.length + value.length);
		expected.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x18 });
		expected.writeInt(key.length);
		expected.writeInt(value.length);
		expected.writeBytes(key);
		expected.writeBytes(value);
		ChannelBuffer actual = ChannelBuffers.buffer(expected.capacity());
		dut.encode(actual);
		assertEquals(expected, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertTrue(dut.decode(response));
		dut.getReturnValue();
	}

	@Test public void out() {
		Out dut = new Out(key);
		setupTranscoders(dut);
		
		ChannelBuffer expected = ChannelBuffers.buffer(2 + 4 + key.length);
		expected.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x20 });
		expected.writeInt(key.length);
		expected.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(expected.capacity());
		dut.encode(actual);
		assertEquals(expected, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void get() {
		Get dut = new Get(key);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x30 });
		request.writeInt(key.length);
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + value.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeInt(value.length);
		response.writeBytes(value);
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[])dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void mget() {
		Mget dut = new Mget(new Object[] { key });
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 4 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x31 });
		request.writeInt(1);
		request.writeInt(key.length);
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + 4 + 4 + key.length + value.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeInt(1);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeInt(key.length);
		response.writeInt(value.length);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeBytes(key);
		response.writeBytes(value);
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[]) dut.getReturnValue().values().iterator().next());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		response.writeInt(0);
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void vsiz() {
		Vsiz dut = new Vsiz(key);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x38 });
		request.writeInt(key.length);
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + value.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeInt(value.length);
		assertTrue(dut.decode(response));
		assertEquals(value.length, (int)dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertEquals(-1, (int)dut.getReturnValue());
	}

	@Test public void iterinit() {
		Iterinit dut = new Iterinit();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x50 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void iternext() {
		Iternext dut = new Iternext();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x51 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + value.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeInt(value.length);
		response.writeBytes(value);
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[])dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void fwmkeys() {
		Fwmkeys dut = new Fwmkeys(key, Integer.MAX_VALUE);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 4 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x58 });
		request.writeInt(key.length);
		request.writeInt(Integer.MAX_VALUE);
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + 4 + key.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeInt(1);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeInt(key.length);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeBytes(key);
		assertTrue(dut.decode(response));
		assertEquals(1, dut.getReturnValue().size());
		assertArrayEquals(key, (byte[]) dut.getReturnValue().get(0));
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		response.writeInt(0);
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void addint() {
		int num = 4;
		Addint dut = new Addint(key, num);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 4 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x60 });
		request.writeInt(key.length);
		request.writeInt(num);
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeInt(3 + num);
		assertTrue(dut.decode(response));
		assertEquals(3 + num, (int)dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertEquals(Integer.MIN_VALUE, (int)dut.getReturnValue());
	}

	@Test public void adddouble() {
		double num = 4;
		Adddouble dut = new Adddouble(key, num);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 8 + 8 + key.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x61 });
		request.writeInt(key.length);
		request.writeLong(dut._integ(num));
		request.writeLong(dut._fract(num));
		request.writeBytes(key);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 8 + 8);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeLong(dut._integ(3.0 + num));
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeLong(dut._fract(3.0 + num));
		assertTrue(dut.decode(response));
		assertEquals(3.0 + num, (double)dut.getReturnValue(), 0.0);
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertEquals(Double.NaN, (double)dut.getReturnValue(), 0.0);
	}

	@Test public void ext() {
		String name = "function";
		Ext dut = new Ext(name, key, value, RDB.XOLCKREC);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 4 + 4 + 4 + name.getBytes().length + key.length + value.length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x68 });
		request.writeInt(name.getBytes().length);
		request.writeInt(RDB.XOLCKREC);
		request.writeInt(key.length);
		request.writeInt(value.length);
		request.writeBytes(name.getBytes());
		request.writeBytes(key);
		request.writeBytes(value);
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + value.length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeInt(value.length);
		response.writeBytes(value);
		assertTrue(dut.decode(response));
		assertArrayEquals(value, (byte[])dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertNull(dut.getReturnValue());
	}

	@Test public void sync() {
		Sync dut = new Sync();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x70 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void vanish() {
		Vanish dut = new Vanish();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x71 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void copy() {
		String path = "path";
		Copy dut = new Copy(path);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + path.getBytes().length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x72 });
		request.writeInt(path.getBytes().length);
		request.writeBytes(path.getBytes());
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void restore() {
		String path = "path";
		long timestamp = System.currentTimeMillis();
		Restore dut = new Restore(path, timestamp);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 8 + path.getBytes().length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x73 });
		request.writeInt(path.getBytes().length);
		request.writeLong(timestamp);
		request.writeBytes(path.getBytes());
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void setmst() {
		String host = "host";
		int port = 1978;
		Setmst dut = new Setmst(host, port);
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2 + 4 + 4 + host.getBytes().length);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x78 });
		request.writeInt(host.getBytes().length);
		request.writeInt(port);
		request.writeBytes(host.getBytes());
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertTrue(dut.decode(response));
		assertTrue(dut.getReturnValue());
		
		//error
		response.clear();
		response.writeByte(Command.EUNKNOWN);
		assertTrue(dut.decode(response));
		assertFalse(dut.getReturnValue());
	}

	@Test public void rnum() {
		long rnum = 123;
		Rnum dut = new Rnum();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x80 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 8);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeLong(rnum);
		assertTrue(dut.decode(response));
		assertEquals(rnum, (long)dut.getReturnValue());
	}

	@Test public void size() {
		long size = 12345;
		Size dut = new Size();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x81 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 8);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();
		
		response.writeLong(size);
		assertTrue(dut.decode(response));
		assertEquals(size, (long)dut.getReturnValue());
	}


	@Test public void stat() {
		String stat = "k1\tv1\nk2\tv2\n";
		Stat dut = new Stat();
		setupTranscoders(dut);
		
		ChannelBuffer request = ChannelBuffers.buffer(2);
		request.writeBytes(new byte[] { (byte) 0xC8, (byte) 0x88 });
		ChannelBuffer actual = ChannelBuffers.buffer(request.capacity());
		dut.encode(actual);
		assertEquals(request, actual);
		
		ChannelBuffer response = ChannelBuffers.buffer(1 + 4 + stat.getBytes().length);
		assertFalse(dut.decode(response));
		
		response.writeByte(Command.ESUCCESS);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeInt(stat.getBytes().length);
		assertFalse(dut.decode(response));
		response.resetReaderIndex();

		response.writeBytes(stat.getBytes());
		assertTrue(dut.decode(response));
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("k1", "v1");
		expected.put("k2", "v2");
		assertEquals(expected, dut.getReturnValue());
	}
}
