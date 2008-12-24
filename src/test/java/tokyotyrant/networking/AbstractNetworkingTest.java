package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Arrays;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class AbstractNetworkingTest {
	private Mockery mockery = new JUnit4Mockery();
	private AbstractNetworking dut;
	private NodeLocator nodeLocator;

	@Before public void beforeEach() {
		nodeLocator = mockery.mock(NodeLocator.class);
		dut = new AbstractNetworking(nodeLocator) {
			public void setAddresses(URI[] addresses) {
			}
			public void start() {
			}
			public void stop() {
			}
		};
	}
	
	@Test public void selectPrimaryIfItIsActive() {
		final ServerNode node0 = mockery.mock(ServerNode.class, "node0");
		mockery.checking(new Expectations() {{
			one(nodeLocator).getSequence(); will(returnValue(Arrays.asList(node0).iterator()));
			one(node0).isActive(); will(returnValue(true));
		}});
		assertEquals(node0, dut.selectNode());
	}

	@Test public void selectBackupIfThePrimaryIsNotActive() {
		final ServerNode node0 = mockery.mock(ServerNode.class, "node0");
		final ServerNode node1 = mockery.mock(ServerNode.class, "node1");
		mockery.checking(new Expectations() {{
			one(nodeLocator).getSequence(); will(returnValue(Arrays.asList(node0, node1).iterator()));
			one(node0).isActive(); will(returnValue(false));
			one(node1).isActive(); will(returnValue(true));
		}});
		assertEquals(node1, dut.selectNode());
	}

	@Test public void selectPrimaryIfAllBackupsAreNotActive() {
		final ServerNode node0 = mockery.mock(ServerNode.class, "node0");
		final ServerNode node1 = mockery.mock(ServerNode.class, "node1");
		mockery.checking(new Expectations() {{
			one(nodeLocator).getSequence(); will(returnValue(Arrays.asList(node0, node1).iterator()));
			one(node0).isActive(); will(returnValue(false));
			one(node1).isActive(); will(returnValue(false));
		}});
		assertEquals(node0, dut.selectNode());
	}
}
