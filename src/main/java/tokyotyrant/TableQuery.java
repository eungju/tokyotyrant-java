package tokyotyrant;

import java.util.ArrayList;
import java.util.List;

public class TableQuery {
	/** query condition: string is equal to */
	public static final int QCSTREQ = 0;
	/** query condition: string is included in */
	public static final int QCSTRINC = 1;
	/** query condition: string begins with */
	public static final int QCSTRBW = 2;
	/** query condition: string ends with */
	public static final int QCSTREW = 3;
	/** query condition: string includes all tokens in */
	public static final int QCSTRAND = 4;
	/** query condition: string includes at least one token in */
	public static final int QCSTROR = 5;
	/** query condition: string is equal to at least one token in */
	public static final int QCSTROREQ = 6;
	/** query condition: string matches regular expressions of */
	public static final int QCSTRRX = 7;
	/** query condition: number is equal to */
	public static final int QCNUMEQ = 8;
	/** query condition: number is greater than */
	public static final int QCNUMGT = 9;
	/** query condition: number is greater than or equal to */
	public static final int QCNUMGE = 10;
	/** query condition: number is less than */
	public static final int QCNUMLT = 11;
	/** query condition: number is less than or equal to */
	public static final int QCNUMLE = 12;
	/** query condition: number is between two tokens of */
	public static final int QCNUMBT = 13;
	/** query condition: number is equal to at least one token in */
	public static final int QCNUMOREQ = 14;
	/** query condition: full-text search with the phrase of */
	public static final int QCFTSPH = 15;
	/** query condition: full-text search with all tokens in */
	public static final int QCFTSAND = 16;
	/** query condition: full-text search with at least one token in */
	public static final int QCFTSOR = 17;
	/** query condition: full-text search with the compound expression of */
	public static final int QCFTSEX = 18;
	/** query condition: negation flag */
	public static final int QCNEGATE = 1 << 24;
	/** query condition: no index flag */
	public static final int QCNOIDX = 1 << 25;

    /** order type: string ascending */
    public static final int QOSTRASC = 0;
    /** order type: string descending */
    public static final int QOSTRDESC = 1;
    /** order type: number ascending */
    public static final int QONUMASC = 2;
    /** order type: number descending */
    public static final int QONUMDESC = 3;

	public List<Condition> conditions = new ArrayList<Condition>();
	public Limit limit;
	public Order order;
	
	public TableQuery condition(String name, int op, String expr) {
		conditions.add(new Condition(name, op, expr));
		return this;
	}

	public TableQuery order(String name, int type) {
		order = new Order(name, type);
		return this;
	}

	public TableQuery limit(int max, int skip) {
		this.limit = new Limit(max, skip);
		return this;
	}

	static class Condition {
		public final String name;
		public final int op;
		public final String expr;

		public Condition(String name, int op, String expr) {
			this.name = name;
			this.op = op;
			this.expr = expr;
		}
		
		@Override
		public boolean equals(Object o) {
			Condition other = (Condition) o;
			return name.equals(other.name) && op == other.op && expr.equals(other.expr);
		}
	}
	
	static class Limit {
		public final int max;
		public final int skip;
		
		public Limit(int max, int skip) {
			this.max = max;
			this.skip = skip;
		}
		
		@Override
		public boolean equals(Object o) {
			Limit other = (Limit) o;
			return max == other.max && skip == other.skip;
		}
	}
	
	static class Order {
		public final String name;
		public final int type;
		
		public Order(String name, int type) {
			this.name = name;
			this.type = type;
		}
		
		@Override
		public boolean equals(Object o) {
			Order other = (Order) o;
			return name.equals(other.name) && type == other.type;
		}
	}
}
