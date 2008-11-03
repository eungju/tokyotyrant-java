package org.zact.tokyotyrant;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class PurnrTest {
	@Test public void shouldNotExpectResponse() {
		Putnr command = new Putnr("key", "value");
		assertTrue(command.decode(ByteBuffer.allocate(1)));
	}
}
