package tokyotyrant.networking.nio;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import tokyotyrant.networking.NodeAddress;
import tokyotyrant.protocol.Command;
import tokyotyrant.protocol.PingCommand;

@RunWith(JMock.class)
public class NioNodeTest {
	private Mockery mockery = new JUnit4Mockery() {{
		this.setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private NioNode dut;
	private SocketChannel channel;
	private NodeAddress address;
	
	@Before public void beforeEach() {
		channel = mockery.mock(SocketChannel.class);
		dut = new NioNode(null) {
			public void fixupInterests() {}
		};
		address = new NodeAddress("tcp://localhost:1978");
		dut.initialize(address);
		dut.channel = channel;
	}
	
	@Test public void handleConnect() throws Exception {
		mockery.checking(new Expectations() {{
			one(channel).finishConnect(); will(returnValue(true));
		}});
		dut.handleConnect();
	}
	
	@Test public void handleWrite() throws Exception {
		mockery.checking(new Expectations() {{
			one(channel).write(with(any(ByteBuffer.class))); will(returnValue(0));
		}});
		dut.handleWrite();
	}
	
	@Test public void fillOutgoingBufferWithOneCommand() throws Exception {
		PingCommand command = new PingCommand(1);
		dut.writingCommands.add(command);
		dut.fillOutgoingBuffer();
		assertTrue(command.isReading());
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		command.encode(buffer);
		assertEquals(buffer, dut.outgoingBuffer);
	}

	@Test public void fillOutgoingBufferShouldNotExceedHighwatermark() throws Exception {
		dut.outgoingBuffer.writeBytes(new byte[address.bufferHighwatermark()]);
		PingCommand command = new PingCommand(1);
		dut.writingCommands.add(command);
		dut.fillOutgoingBuffer();
		assertEquals(address.bufferHighwatermark(), dut.outgoingBuffer.readableBytes());
	}

	@Test public void handleRead() throws Exception {
		mockery.checking(new Expectations() {{
			one(channel).read(with(any(ByteBuffer.class))); will(returnValue(0));
		}});
		dut.handleRead();
	}
	
	@Test public void consumeIncomingBuffer() throws Exception {
		PingCommand command = new PingCommand(1);
		command.reading();
		dut.readingCommands.add(command);
		dut.incomingBuffer.writeByte(Command.EUNKNOWN);
		dut.consumeIncomingBuffer();
		assertTrue(command.isCompleted());
	}
}
