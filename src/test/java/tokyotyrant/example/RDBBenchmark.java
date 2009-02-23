package tokyotyrant.example;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.SerializingTranscoder;

public class RDBBenchmark {
	public static void main(String[] args) throws IOException {
		RDB rdb = new RDB();
		rdb.open(URI.create(args[0]));
		rdb.setValueTranscoder(new SerializingTranscoder());
		byte[] value = new byte[128];
		rdb.put("key", value);
		StopWatch watch = new StopWatch().start();
		for (int i = 0; i < 1000; i++) {
			assertArrayEquals(value, (byte[]) rdb.get("key"));
		}
		System.out.println(watch.stop().taken() + "ms");
		rdb.close();
	}
}
