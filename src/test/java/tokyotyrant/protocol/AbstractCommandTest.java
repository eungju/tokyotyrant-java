package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.runner.RunWith;

import tokyotyrant.MRDB;
import tokyotyrant.RDB;
import tokyotyrant.networking.Networking;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.Transcoder;

@RunWith(JMock.class)
public abstract class AbstractCommandTest {
	protected Mockery mockery = new JUnit4Mockery();
	protected RDB rdb;
	protected MRDB mrdb;
	protected Networking networking;
	protected Transcoder transcoder = new ByteArrayTranscoder();
	protected byte[] key = "key".getBytes();
	protected byte[] value = "value".getBytes();

	@Before public void beforeEach() throws Exception {
		rdb = new RDB() {
			@Override
			protected <T> T execute(Command<T> command) {
				return command.getReturnValue();
			}
		};
		networking = mockery.mock(Networking.class);
		mrdb = new MRDB(networking);
	}

	void putFamily(PutCommandSupport dut, int commandId) {
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
	
}
