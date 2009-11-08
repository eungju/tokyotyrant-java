package tokyotyrant.example;

import tokyotyrant.MRDB;
import tokyotyrant.networking.NodeAddress;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.StringTranscoder;

public class MRDBBenchmark {
	public static void main(String[] args) throws Exception {
		final MRDB db = new MRDB();
		db.open(NodeAddress.addresses(args[0]));
		db.setGlobalTimeout(Long.MAX_VALUE);
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
					db.get(key).get();
				} catch (Exception e) {
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
