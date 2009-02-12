package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.net.URI;

import org.junit.Test;

public class NioNodeTest {
	@Test public void readOnlyDisabled() {
		NioNode dut = new NioNode(null);
		dut.initialize(URI.create("tcp://localhost:1978"));
		assertFalse(dut.isReadOnly());
	}
	
	@Test public void readOnlyEnabled() {
		NioNode dut = new NioNode(null);
		dut.initialize(URI.create("tcp://localhost:1978/?readOnly=true"));
		assertTrue(dut.isReadOnly());
	}
}
