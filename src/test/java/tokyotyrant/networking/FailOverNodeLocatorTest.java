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
public class FailOverNodeLocatorTest {
	private Mockery mockery = new JUnit4Mockery();
	private FailOverNodeLocator dut;
	private TokyoTyrantNode node0;
	private TokyoTyrantNode node1;
	
	@Before public void beforeEach() {
		dut = new FailOverNodeLocator();
		node0 = mockery.mock(TokyoTyrantNode.class, "node0");
		node1 = mockery.mock(TokyoTyrantNode.class, "node1");
		dut.setNodes(Arrays.asList(node0, node1));
	}
	
	@Test public void getAll() {
		assertEquals(Arrays.asList(node0, node1), dut.getAll());
	}
	
	@Test public void getPrimary() {
		assertSame(node0, dut.getPrimary());
	}

	@Test public void getSequence() {
		Iterator<TokyoTyrantNode> i = dut.getSequence();
		assertEquals(node1, i.next());
		assertFalse(i.hasNext());
	}

	@Test(expected=UnsupportedOperationException.class)
	public void sequenceIsImmutable() {
		Iterator<TokyoTyrantNode> i = dut.getSequence();
		i.remove();
	}
}
