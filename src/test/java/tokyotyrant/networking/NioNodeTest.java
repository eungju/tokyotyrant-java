package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.net.URI;

import org.junit.Test;

public class NioNodeTest {
	@Test public void readOnlyOption() {
		assertFalse(new NioNode(URI.create("tcp://localhost:1978"), null).isReadOnly());
		assertTrue(new NioNode(URI.create("tcp://localhost:1978/?readOnly=true"), null).isReadOnly());
	}
}
