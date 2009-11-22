package tokyotyrant;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import tokyotyrant.transcoder.StringTranscoder;

/**
 * The table database API.
 */
public class RDBTable {
    /**
     * index type: lexical string
     */
    public static final int ITLEXICAL = 0;
    /**
     * index type: decimal string
     */
    public static final int ITDECIMAL = 1;
    /**
     * index type: token inverted index
     */
    public static final int ITTOKEN = 2;
    /**
     * index type: q-gram inverted index
     */
    public static final int ITQGRAM = 3;
    /**
     * index type: optimize
     */
    public static final int ITOPT = 9998;
    /**
     * index type: void
     */
    public static final int ITVOID = 9999;
    /**
     * index type: keep existing index
     */
    public static final int ITKEEP = 1 << 24;
	
	private final RDB db;
	private final StringTranscoder stringTranscoder;

	public RDBTable(RDB db) {
		this.db = db;
		this.stringTranscoder = new StringTranscoder();
	}

	/**
	 * Store a record. If a record with the same key exists in the database, it is overwritten.
	 * @param pkey specifies the primary key.
	 * @param cols specifies a hash containing columns.
	 * @return If successful, the return value is {@code true}, else, it is {@code false}.
	 */
	public boolean put(String pkey, Map<String, String> cols) {
		List<byte[]> args = new ArrayList<byte[]>();
		args.add(stringTranscoder.encode(pkey));
		for (Map.Entry<String, String> each : cols.entrySet()) {
			args.add(stringTranscoder.encode(each.getKey()));
			args.add(stringTranscoder.encode(each.getValue()));
		}
		List<byte[]> rv = db.misc("put", args, 0);
		return rv != null;
	}

	/**
	 * Remove a record.
	 * @param pkey specifies the primary key.
	 * @return If successful, the return value is {@code true}, else, it is {@code false}.
	 */
	public boolean out(String pkey) {
		List<byte[]> args = new ArrayList<byte[]>();
		args.add(stringTranscoder.encode(pkey));
		List<byte[]> rv = db.misc("out", args, 0);
		return rv != null;
	}

	/**
	 * Retrieve a record.
	 * @param pkey specifies the primary key.
	 * @return If successful, the return value is a hash of the columns of the corresponding record. {@code null} is returned if no record corresponds.
	 */
	public Map<String, String> get(String pkey) {
		List<byte[]> args = new ArrayList<byte[]>();
		args.add(stringTranscoder.encode(pkey));
		List<byte[]> rv = db.misc("get", args, RDB.MONOULOG);
		if (rv == null) {
			return null;
		}
		Map<String, String> result = new HashMap<String, String>();
		Iterator<byte[]> i = rv.iterator();
		while (i.hasNext()) {
			String ckey = (String) stringTranscoder.decode(i.next());
			String cvalue = (String) stringTranscoder.decode(i.next());
			result.put(ckey, cvalue);
		}
		return result;
	}

	/**
	 * Set a column index.
	 * 
	 * @param name
	 *            specifies the name of a column. If the name of an existing
	 *            index is specified, the index is rebuilt. An empty string
	 *            means the primary key.
	 * @param type
	 *            specifies the index type: {@link RDBTable#ITLEXICAL} for
	 *            lexical string, {@link RDBTable#ITDECIMAL} for decimal string,
	 *            {@link RDBTable#ITTOKEN} for token inverted index,
	 *            {@link RDBTable#ITQGRAM} for q-gram inverted index. If it is
	 *            {@link RDBTable#ITOPT}, the index is optimized. If it is
	 *            {@link RDBTable#ITVOID}, the index is removed. If
	 *            {@link RDBTable#ITKEEP} is added by bitwise-or and the index
	 *            exists, this method merely returns failure.
	 * @return If successful, the return value is {@code true}, else, it is
	 *         {@code false}.
	 */
	public boolean setindex(String name, int type) {
		List<byte[]> args = new ArrayList<byte[]>();
		args.add(stringTranscoder.encode(name));
		args.add(stringTranscoder.encode(type));
		List<byte[]> rv = db.misc("setindex", args, 0);
		return rv != null;
	}
	
    /**
     * Generate a unique ID number.
     * @return The return value is the new unique ID number or -1 on failure.
     */
    public long genuid() {
      List<byte[]> rv = db.misc("genuid", Collections.<byte[]>emptyList(), 0);
      if (rv == null) {
    	  return -1;
      }
      return Long.parseLong((String) stringTranscoder.decode(rv.get(0)));
    }
    
    public List<String> search(TableQuery query) {
    	List<byte[]> args = new ArrayList<byte[]>();
    	for (TableQuery.Condition each : query.conditions) {
			args.add(encodeCondition(each));
    	}
    	if (query.order != null) {
			args.add(encodeSetorder(query.order));
    	}
    	if (query.limit != null) {
			args.add(encodeSetlimit(query.limit));
    	}
    	query.hint = "";
    	List<byte[]> rv = db.misc("search", args, RDB.MONOULOG);
    	if (rv == null) {
    		Collections.emptyList();
    	}
    	List<String> result = new ArrayList<String>();
    	for (byte[] each : rv) {
    		String pkey = (String) stringTranscoder.decode(each);
    		if (pkey.startsWith("\0\0[[HINT]]\n")) {
    			query.hint = pkey.substring("\0\0[[HINT]]\n".length());
    		} else {
    			result.add(pkey);
    		}
    	}
    	return result;
    }
    
    byte[] encodeCondition(TableQuery.Condition condition) {
		byte[] addcond = "addcond".getBytes();
		byte[] name = stringTranscoder.encode(condition.name);
		byte[] op = stringTranscoder.encode(condition.op);
		byte[] expr = stringTranscoder.encode(condition.expr);
		ByteBuffer buf = ByteBuffer.allocate(addcond.length + 1 + name.length + 1 + op.length + 1 + expr.length);
		buf.put(addcond);
		buf.put((byte) 0).put(name);
		buf.put((byte) 0).put(op);
		buf.put((byte) 0).put(expr);
		return buf.array();
    }
    
    byte[] encodeSetorder(TableQuery.Order order) {
		byte[] setorder = "setorder".getBytes();
		byte[] name = stringTranscoder.encode(order.name);
		byte[] type = stringTranscoder.encode(order.type);
		ByteBuffer buf = ByteBuffer.allocate(setorder.length + 1 + name.length + 1 + type.length);
		buf.put(setorder);
		buf.put((byte) 0).put(name);
		buf.put((byte) 0).put(type);
		return buf.array();
    }
    
    byte[] encodeSetlimit(TableQuery.Limit limit) {
		byte[] setlimit = "setlimit".getBytes();
		byte[] max = stringTranscoder.encode(limit.max);
		byte[] skip = stringTranscoder.encode(limit.skip);
		ByteBuffer buf = ByteBuffer.allocate(setlimit.length + 1 + max.length + 1 + skip.length);
		buf.put(setlimit);
		buf.put((byte) 0).put(max);
		buf.put((byte) 0).put(skip);
		return buf.array();
    }    
}
