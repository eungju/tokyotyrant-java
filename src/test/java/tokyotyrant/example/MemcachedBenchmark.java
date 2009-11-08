package tokyotyrant.example;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

public class MemcachedBenchmark {
	public static void main(String[] args) throws Exception {
		final MemcachedClient db = new MemcachedClient(AddrUtil.getAddresses(args[0]));
		final String key = "key";
		final byte[] value = new byte[128];
		value[0] = 1;
		value[1] = 2;
		value[2] = 3;
		db.set(key, 0, value);
		Runnable task = new Runnable() {
			public void run() {
				try {
					db.asyncGet(key).get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		for (int c : new int[] {1, 10, 100}) {
			System.out.println(c + ":" + (new Benchmark(c, 10000).run(task)) + "ms");
		}
		db.shutdown();
	}
}
