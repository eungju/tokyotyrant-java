package tokyotyrant.example;

import static org.junit.Assert.*;

import java.util.concurrent.Future;

import tokyotyrant.MRDB;
import tokyotyrant.helper.UriHelper;
import tokyotyrant.transcoder.SerializingTranscoder;

public class MRDBBenchmark {
	public static void main(String[] args) throws Exception {
		MRDB db = new MRDB();
		db.open(UriHelper.getUris(args[0]));
		db.setGlobalTimeout(Long.MAX_VALUE);
		db.setValueTranscoder(new SerializingTranscoder());
		byte[] value = new byte[128];
		value[0] = 1;
		value[1] = 2;
		value[2] = 3;
		db.put("key", value);
		StopWatch watch = new StopWatch().start();
		Future<?>[] futures = new Future[10000];
		for (int i = 0; i < futures.length; i++) {
			futures[i] = db.get("key");
		}
		for (int i = 0; i < futures.length; i++) {
			System.out.println(i);
			assertArrayEquals(value, (byte[]) futures[i].get());
		}
		System.out.println(watch.stop().taken() + "ms");
		db.close();
	}
}
