package tokyotyrant.example;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.concurrent.Future;

import org.junit.Ignore;
import org.junit.Test;

import tokyotyrant.MRDB;
import tokyotyrant.transcoder.SerializingTranscoder;

@Ignore
public class MRDBBenchmark {
	@Test public void get() throws Exception {
		MRDB db = new MRDB(new URI[] { URI.create("tcp://localhost:1978") });
		db.setGlobalTimeout(Long.MAX_VALUE);
		db.setValueTranscoder(new SerializingTranscoder());
		byte[] value = new byte[128];
		value[0] = 1;
		value[1] = 2;
		value[2] = 3;
		db.put("key", value);
		Future<?>[] futures = new Future[10000];
		for (int i = 0; i < futures.length; i++) {
			futures[i] = db.get("key");
		}
		for (int i = 0; i < futures.length; i++) {
			System.out.println(i);
			assertArrayEquals(value, (byte[]) futures[i].get());
		}
		db.dispose();
	}
}
