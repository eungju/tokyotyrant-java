package tokyotyrant.example;

import java.io.IOException;
import java.net.InetSocketAddress;

import tokyotyrant.RDB;

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

		// close the connection
		rdb.close();
	}
}
