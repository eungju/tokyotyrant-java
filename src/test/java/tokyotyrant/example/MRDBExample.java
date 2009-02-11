package tokyotyrant.example;

import java.net.URI;

import tokyotyrant.MRDB;
import tokyotyrant.transcoder.DoubleTranscoder;
import tokyotyrant.transcoder.IntegerTranscoder;

public class MRDBExample {
	public static void main(String[] args) throws Exception {
		Object value;

		// open the connections
		MRDB db = new MRDB(new URI[] { new URI("tcp://localhost:1978") });

		// store records
		if (!db.await(db.put("foo", "hop"))
				|| !db.await(db.put("bar", "step"))
				|| !db.await(db.put("baz", "jump"))) {
			System.err.println("put error");
		}

		// retrieve records
		value = db.await(db.get("foo"));
		if (value != null) {
			System.out.println(value);
		} else {
			System.err.println("get error");
		}

		// add int
		db.put("int", 3, new IntegerTranscoder());
		int i = db.await(db.addint("int", 4));
		System.out.println(i);

		// add double
		db.put("d", 3.0D, new DoubleTranscoder());
		double d = db.await(db.adddouble("d", 4.0D));
		System.out.println(d);

		// close the connections
		db.dispose();
	}
}
