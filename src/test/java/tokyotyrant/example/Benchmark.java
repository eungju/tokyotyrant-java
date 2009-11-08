package tokyotyrant.example;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Benchmark {
	private int concurrency;
	private int number;

	public Benchmark(int concurrency, int number) {
		this.concurrency = concurrency;
		this.number = number;
	}
	
	public long run(Runnable task) {
		ThreadPoolExecutor executor = new ThreadPoolExecutor(concurrency, concurrency, Long.MAX_VALUE, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		StopWatch watch = new StopWatch().start();
		for (int i = 0; i < number; i++) {
			executor.execute(task);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return watch.stop().taken();
	}
}
