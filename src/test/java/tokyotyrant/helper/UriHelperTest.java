package tokyotyrant.helper;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class UriHelperTest {
	URI uri(String str) {
		return URI.create(str);
	}
	
	@Test public void getUrisOfSingleNode() {
		assertArrayEquals(new URI[] { uri("tcp://localhost:1978") }, UriHelper.getUris("tcp://localhost:1978"));
	}

	@Test public void getUrisOfMultipleNodes() {
		assertArrayEquals(new URI[] { uri("tcp://localhost:1978"), uri("tcp://localhost:1979") },
				UriHelper.getUris("tcp://localhost:1978 tcp://localhost:1979"));
	}
	
	@Test public void getSocketAddress() {
		assertEquals(new InetSocketAddress("localhost", 1978), UriHelper.getSocketAddress(uri("tcp://localhost:1978")));
	}
	
	@Test public void getParameters() {
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("name", "value");
		assertEquals(expected, UriHelper.getParameters(uri("tcp://localhost/?name=value")));
	}
}
