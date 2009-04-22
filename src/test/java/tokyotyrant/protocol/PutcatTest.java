package tokyotyrant.protocol;

import java.io.IOException;

import org.jmock.Expectations;
import org.junit.Test;

public class PutcatTest extends AbstractCommandTest {
	@Test public void protocol() {
		putFamily(new Putcat(transcoder, transcoder, key, value), 0x12);
	}

	@Test public void rdb() throws IOException {
		rdb.putcat(key, value);
		rdb.putcat(key, value, transcoder);
	}
	
	@Test public void mrdb() {
		mockery.checking(new Expectations() {{
			one(networking).send(with(any(Putcat.class)));
			one(networking).send(with(any(Putcat.class)));
		}});
		mrdb.putcat(key, value);
		mrdb.putcat(key, value, transcoder);
	}
}
