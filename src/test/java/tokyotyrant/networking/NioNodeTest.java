package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.net.URI;
import java.nio.channels.SocketChannel;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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
}
