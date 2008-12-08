package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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

	@Test public void putshl() {
		int width = 1;
		Putshl dut = new Putshl(key, value, width);
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

	@Test public void addint() {
		int num = 4;
		Addint dut = new Addint(key, num);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x60 }).putInt(key.length).putInt(num).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putInt(3 + num).flip();
		assertTrue(dut.decode(response));
		assertEquals(3 + num, (int)dut.getReturnValue());
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertEquals(Integer.MIN_VALUE, (int)dut.getReturnValue());
	}

	@Test public void adddouble() {
		double num = 4;
		Adddouble dut = new Adddouble(key, num);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 8 + 8 + key.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x61 }).putInt(key.length).putLong(dut._integ(num)).putLong(dut._fract(num)).put(key);
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 8 + 8);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putLong(dut._integ(3.0 + num)).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putLong(dut._fract(3.0 + num)).flip();
		assertTrue(dut.decode(response));
		assertEquals(3.0 + num, (double)dut.getReturnValue(), 0.0);
		
		//error
		response.clear();
		response.put(Command.EUNKNOWN).flip();
		assertTrue(dut.decode(response));
		assertEquals(Double.NaN, (double)dut.getReturnValue(), 0.0);
	}

	@Test public void ext() {
		String name = "function";
		Ext dut = new Ext(name, key, value, RDB.XOLCKREC);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4 + name.getBytes().length + key.length + value.length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x68 })
			.putInt(name.getBytes().length).putInt(RDB.XOLCKREC).putInt(key.length).putInt(value.length)
			.put(name.getBytes()).put(key).put(value);
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

	@Test public void sync() {
		Sync dut = new Sync();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x70 });
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

	@Test public void vanish() {
		Vanish dut = new Vanish();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x71 });
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

	@Test public void copy() {
		String path = "path";
		Copy dut = new Copy(path);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + path.getBytes().length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x72 }).putInt(path.getBytes().length).put(path.getBytes());
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

	@Test public void restore() {
		String path = "path";
		long timestamp = System.currentTimeMillis();
		Restore dut = new Restore(path, timestamp);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 8 + path.getBytes().length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x73 }).putInt(path.getBytes().length).putLong(timestamp).put(path.getBytes());
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

	@Test public void setmst() {
		String host = "host";
		int port = 1978;
		Setmst dut = new Setmst(host, port);
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2 + 4 + 4 + host.getBytes().length)
			.put(new byte[] { (byte) 0xC8, (byte) 0x78 }).putInt(host.getBytes().length).putInt(port).put(host.getBytes());
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

	@Test public void rnum() {
		long rnum = 123;
		Rnum dut = new Rnum();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x80 });
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 8);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putLong(rnum).flip();
		assertTrue(dut.decode(response));
		assertEquals(rnum, (long)dut.getReturnValue());
	}

	@Test public void size() {
		long size = 12345;
		Size dut = new Size();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x81 });
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 8);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.putLong(size).flip();
		assertTrue(dut.decode(response));
		assertEquals(size, (long)dut.getReturnValue());
	}


	@Test public void stat() {
		String stat = "k1\tv1\nk2\tv2\n";
		Stat dut = new Stat();
		setupTranscoders(dut);
		
		ByteBuffer request = ByteBuffer.allocate(2)
			.put(new byte[] { (byte) 0xC8, (byte) 0x88 });
		assertArrayEquals(request.array(), dut.encode().array());
		
		ByteBuffer response = ByteBuffer.allocate(1 + 4 + stat.getBytes().length);
		response.flip();
		assertFalse(dut.decode(response));
		
		response.limit(response.capacity());
		response.put(Command.ESUCCESS).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.putInt(stat.getBytes().length).flip();
		assertFalse(dut.decode(response));

		response.limit(response.capacity());
		response.put(stat.getBytes()).flip();
		assertTrue(dut.decode(response));
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("k1", "v1");
		expected.put("k2", "v2");
		assertEquals(expected, dut.getReturnValue());
	}
}
