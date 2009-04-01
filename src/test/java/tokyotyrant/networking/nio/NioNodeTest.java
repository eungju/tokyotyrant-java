package tokyotyrant.networking.nio;

import java.nio.channels.SocketChannel;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import tokyotyrant.networking.NodeAddress;

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
		final Outgoing outgoing = mockery.mock(Outgoing.class);
		mockery.checking(new Expectations() {{
			one(outgoing).write();
		}});
		dut.outgoing = outgoing;
		dut.handleWrite();
	}

	@Test public void handleRead() throws Exception {
		final Incoming incoming = mockery.mock(Incoming.class);
		mockery.checking(new Expectations() {{
			one(incoming).read();
		}});
		dut.incoming = incoming;
		dut.handleRead();
	}
}
