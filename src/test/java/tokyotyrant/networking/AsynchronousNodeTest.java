package tokyotyrant.networking;

import static org.junit.Assert.*;

import java.net.URI;

import org.junit.Test;

public class AsynchronousNodeTest {
	@Test public void readOnlyOption() {
		assertFalse(new AsynchronousNode(URI.create("tcp://localhost:1978"), null).isReadOnly());
		assertTrue(new AsynchronousNode(URI.create("tcp://localhost:1978/?readOnly=true"), null).isReadOnly());
	}
}
