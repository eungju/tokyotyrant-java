package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

public class RestoreTest extends AbstractCommandTest {
	@Test public void protocol() {
		String path = "path";
		long timestamp = System.currentTimeMillis();
		Restore dut = new Restore(path, timestamp);
		
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

	@Test public void rdb() throws IOException {
		rdb.restore("path", 123L);
	}
}
