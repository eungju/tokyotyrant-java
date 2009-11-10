package tokyotyrant.example;

import static org.junit.Assert.*;

import java.io.IOException;

import tokyotyrant.RDB;
import tokyotyrant.networking.NodeAddress;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.StringTranscoder;

public class RDBBenchmark {
	public static void main(String[] args) throws Exception {
		final RDB db = new RDB();
		db.open(new NodeAddress(args[0]));
		db.setKeyTranscoder(new StringTranscoder());
		db.setValueTranscoder(new ByteArrayTranscoder());
		final String key = "key";
		final byte[] value = new byte[128];
		value[0] = 1;
		value[1] = 2;
		value[2] = 3;
		db.put(key, value);
		Runnable task = new Runnable() {
			public void run() {
				try {
					synchronized (db) {
						assertArrayEquals(value, (byte[]) db.get(key));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		for (int c : new int[] {1, 10, 100}) {
			System.out.println(c + ":" + (new Benchmark(c, 10000).run(task)) + "ms");
		}
		db.close();
	}
}
