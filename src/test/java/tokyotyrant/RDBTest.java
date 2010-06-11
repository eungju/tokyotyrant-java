package tokyotyrant;

import static org.junit.Assert.*;

import java.io.IOException;

import org.hamcrest.Description;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import tokyotyrant.protocol.Vanish;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.StringTranscoder;
import tokyotyrant.transcoder.Transcoder;

@RunWith(JMock.class)
public class RDBTest {
	private Mockery mockery = new JUnit4Mockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private RDB dut;

	@Before public void beforeEach() {
		dut = new RDB();
	}
	
	@Test public void execute() throws IOException {
		final RDB.Connection connection = mockery.mock(RDB.Connection.class);
		dut.connection = connection;
		Vanish command = new Vanish();
		final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		command.encode(buffer);

		mockery.checking(new Expectations() {{
            one(connection).write(with(any(ChannelBuffer.class)));
            one(connection).read(with(any(ChannelBuffer.class))); will(new Action() {
                public void describeTo(Description description) {
                }

                public Object invoke(Invocation invocation) throws Throwable {
                    ChannelBuffer buffer = (ChannelBuffer) invocation.getParameter(0);
                    buffer.writeBytes(new byte[] { 0 });
                    return null;
                }
            });
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
