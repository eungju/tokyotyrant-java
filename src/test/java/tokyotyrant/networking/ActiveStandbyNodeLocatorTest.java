package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class ActiveStandbyNodeLocatorTest {
	private Mockery mockery = new JUnit4Mockery();
	private ActiveStandbyNodeLocator dut;
	private ServerNode node0;
	private ServerNode node1;
	
	@Before public void beforeEach() {
		dut = new ActiveStandbyNodeLocator();
		node0 = mockery.mock(ServerNode.class, "node0");
		node1 = mockery.mock(ServerNode.class, "node1");
		dut.initialize(new ServerNode[] { node0, node1 });
	}
	
	@Test public void getAll() {
		assertEquals(Arrays.asList(node0, node1), dut.getAll());
	}
	
	@Test public void getSequence() {
		Iterator<ServerNode> i = dut.getSequence();
		assertEquals(node0, i.next());
		assertEquals(node1, i.next());
		assertFalse(i.hasNext());
	}

	@Test(expected=UnsupportedOperationException.class)
	public void sequenceIsImmutable() {
		Iterator<ServerNode> i = dut.getSequence();
		i.remove();
	}
}
