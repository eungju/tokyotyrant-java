package tokyotyrant.networking;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import tokyotyrant.protocol.Command;
import tokyotyrant.protocol.Vanish;

@RunWith(JMock.class)
public class NioNodeTest {
	private Mockery mockery = new JUnit4Mockery() {{
		this.setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private NioNode dut;
	private SocketChannel channel;
	
	@Before public void beforeEach() {
		channel = mockery.mock(SocketChannel.class);
		dut = new NioNode(null) {
			void fixupOperations() {}
		};
		dut.channel = channel;
	}
	
	@Test public void readOnlyDisabled() {
		dut.initialize(URI.create("tcp://localhost:1978"));
		assertFalse(dut.isReadOnly());
	}
	
	@Test public void readOnlyEnabled() {
		dut.initialize(URI.create("tcp://localhost:1978/?readOnly=true"));
		assertTrue(dut.isReadOnly());
	}
	
	@Test public void handleConnect() throws Exception {
		mockery.checking(new Expectations() {{
			one(channel).finishConnect(); will(returnValue(true));
		}});
		dut.handleConnect();
	}
	
	@Test public void handleWriteShouldFillTheBufferWhenTheBufferIsEmpty() throws Exception {
		Command<?> command = new Vanish();
		final ByteBuffer request = command.encode();

		mockery.checking(new Expectations() {{
			one(channel).write(request); will(returnValue(0));
		}});
		dut.writingCommands.add(new Vanish());
		dut.writingBuffer = null;
		dut.handleWrite();
	}

	@Test public void handleWriteShouldNotFillTheBufferWhenTheBufferIsNotEmpty() throws Exception {
		Command<?> command = new Vanish();
		final ByteBuffer request = command.encode();
		request.get();
		
		mockery.checking(new Expectations() {{
			one(channel).write(request); will(returnValue(0));
		}});
		dut.writingCommands.add(command);
		dut.writingBuffer = request;
		dut.handleWrite();
	}

	@Test public void handleWrite() throws Exception {
		Command<?> command = new Vanish();
		final ByteBuffer request = command.encode();
		request.get(new byte[request.capacity()]);
		
		mockery.checking(new Expectations() {{
			one(channel).write(request); will(returnValue(0));
		}});
		dut.writingCommands.add(command);
		dut.writingBuffer = request;
		dut.handleWrite();
		assertNull(dut.writingBuffer);
		assertEquals(0, dut.writingCommands.size());
		assertTrue(command.isReading());
		assertEquals(1, dut.readingCommands.size());
	}
}
