package tokyotyrant;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import tokyotyrant.command.Vanish;

@RunWith(JMock.class)
public class RDBTest {
	private Mockery mockery = new JUnit4Mockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private RDB dut;
	private InputStream inputStream;
	private OutputStream outputStream;

	@Before public void beforeEach() {
		dut = new RDB();
		inputStream = mockery.mock(InputStream.class);
		dut.inputStream = inputStream;
		outputStream = mockery.mock(OutputStream.class);
		dut.outputStream = outputStream;
	}
	
	@Test public void execute() throws IOException {
		Vanish command = new Vanish();
		final ByteBuffer buffer = command.encode();

		mockery.checking(new Expectations() {{
			one(outputStream).write(with(any(byte[].class)), with(equal(0)), with(equal(buffer.limit())));
			one(inputStream).read(with(any(byte[].class)), with(equal(0)), with(equal(2048))); will(returnValue(0));
			one(inputStream).read(with(any(byte[].class)), with(equal(0)), with(equal(2048))); will(returnValue(1));
		}});
		
		assertTrue(dut.execute(command));
	}
	
	@Test public void defaultKeyTranscoderIsStringTranscoder() {
		assertEquals(StringTranscoder.class, dut.getKeyTranscoder().getClass());
	}

	@Test public void keyTranscoderCanBeChanged() {
		Transcoder newTranscoder = new ByteArrayTranscoder();
		dut.setKeyTranscoder(newTranscoder);
		assertEquals(newTranscoder.getClass(), dut.getKeyTranscoder().getClass());
	}
	
	@Test public void defaultValueTranscoderIsStringTranscoder() {
		assertEquals(StringTranscoder.class, dut.getValueTranscoder().getClass());
	}

	@Test public void valueTranscoderCanBeChanged() {
		Transcoder newTranscoder = new ByteArrayTranscoder();
		dut.setValueTranscoder(newTranscoder);
		assertEquals(newTranscoder.getClass(), dut.getValueTranscoder().getClass());
	}
}
