package tokyotyrant.example;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tokyotyrant.RDB;
import tokyotyrant.RDBTable;
import tokyotyrant.TableQuery;

public class TableExample {
	public static void main(String[] args) {
		RDB db = new RDB();
		db.open(new InetSocketAddress("localhost", 1978));
		db.vanish();
		RDBTable table = new RDBTable(db);
		
		// Store a record
		String pkey = String.valueOf(table.genuid());
		Map<String, String> cols = new HashMap<String, String>();
		cols.put("name", "mikio");
		cols.put("age", "30");
		cols.put("lang", "ja,en,c");
		table.put(pkey, cols);

		// Search for records
		TableQuery query = new TableQuery();
		query.condition("age", TableQuery.QCNUMGE, "20");
		query.condition("lang", TableQuery.QCSTROR, "ja,en");
		query.order("name", TableQuery.QOSTRASC);
		query.limit(10, 0);
		List<String> result = table.search(query);
		for (String each : result) {
			System.out.println(each + ":" + table.get(each));
		}
		System.out.println(table.searchCount(query) + " records");
		
		db.close();
	}
}
