package tokyotyrant.example;

import java.io.IOException;
import java.net.InetSocketAddress;

import tokyotyrant.RDB;
import tokyotyrant.transcoder.DoubleTranscoder;
import tokyotyrant.transcoder.IntegerTranscoder;

public class RDBExample {
	public static void main(String[] args) throws IOException {
		Object key;
		Object value;

		// create the object
		RDB rdb = new RDB();

		// connect to the server
		rdb.open(new InetSocketAddress("localhost", 1978));

		// store records
		if (!rdb.put("foo", "hop")
				|| !rdb.put("bar", "step")
				|| !rdb.put("baz", "jump")) {
			System.err.println("put error");
		}

		// retrieve records
		value = rdb.get("foo");
		if (value != null) {
			System.out.println(value);
		} else {
			System.err.println("get error");
		}

		// traverse records
		rdb.iterinit();
		while ((key = rdb.iternext()) != null) {
			value = rdb.get(key);
			if (value != null) {
				System.out.println(key + ":" + value);
			}
		}
		
		// add int
		rdb.put("int", 3, new IntegerTranscoder());
		int i = rdb.addint("int", 4);
		System.out.println(i);

		// add double
		rdb.put("d", 3.0D, new DoubleTranscoder());
		double d = rdb.adddouble("d", 4.0D);
		System.out.println(d);

		// close the connection
		rdb.close();
	}
}
