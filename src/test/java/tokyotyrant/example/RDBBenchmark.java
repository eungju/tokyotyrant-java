package tokyotyrant.example;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Ignore;
import org.junit.Test;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.SerializingTranscoder;

@Ignore
public class RDBBenchmark {
	@Test public void get() throws IOException {
		RDB rdb = new RDB();
		rdb.open(new InetSocketAddress("localhost", 1978));
		rdb.setValueTranscoder(new SerializingTranscoder());
		byte[] value = new byte[4096];
		rdb.put("key", value);
		for (int i = 0; i < 1000; i++) {
			assertArrayEquals(value, (byte[]) rdb.get("key"));
		}
		rdb.close();
	}
}
