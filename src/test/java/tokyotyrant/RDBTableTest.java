package tokyotyrant;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class RDBTableTest {
	private Mockery mockery = new JUnit4Mockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private RDBTable dut;
	private RDB db;
	
	@Before public void beforeEach() {
		db = mockery.mock(RDB.class);
		dut = new RDBTable(db);
	}

	@Test public void putSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("put")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes(), "ckey".getBytes(), "cvalue".getBytes()))), with(equal(0)));
				will(returnValue(Collections.emptyList()));
		}});
		Map<String, String> columns = new HashMap<String, String>();
		columns.put("ckey", "cvalue");
		assertTrue(dut.put("pkey", columns));
	}

	@Test public void putFailure() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("put")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes(), "ckey".getBytes(), "cvalue".getBytes()))), with(equal(0)));
				will(returnValue(null));
		}});
		Map<String, String> columns = new HashMap<String, String>();
		columns.put("ckey", "cvalue");
		assertFalse(dut.put("pkey", columns));
	}

	@Test public void outSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("out")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes()))), with(equal(0)));
				will(returnValue(Collections.emptyList()));
		}});
		assertTrue(dut.out("pkey"));
	}

	@Test public void outFailure() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("out")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes()))), with(equal(0)));
				will(returnValue(null));
		}});
		assertFalse(dut.out("pkey"));
	}

	@Test public void getSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("get")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes()))), with(equal(RDB.MONOULOG)));
				will(returnValue(Arrays.asList("ckey".getBytes(), "cvalue".getBytes())));
		}});
		Map<String, String> columns = new HashMap<String, String>();
		columns.put("ckey", "cvalue");
		assertEquals(columns, dut.get("pkey"));
	}

	@Test public void getFailure() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("get")), with(new MiscListMatcher(Arrays.asList("pkey".getBytes()))), with(equal(RDB.MONOULOG)));
				will(returnValue(null));
		}});
		assertNull(dut.get("pkey"));
	}

	@Test public void setindexSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("setindex")), with(new MiscListMatcher(Arrays.asList("column".getBytes(), "0".getBytes()))), with(equal(0)));
				will(returnValue(Collections.emptyList()));
		}});
		assertTrue(dut.setindex("column", RDBTable.ITLEXICAL));
	}
	
	@Test public void setindexFailure() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("setindex")), with(new MiscListMatcher(Arrays.asList("column".getBytes(), "0".getBytes()))), with(equal(0)));
				will(returnValue(null));
		}});
		assertFalse(dut.setindex("column", RDBTable.ITLEXICAL));
	}

	@Test public void genuidSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc("genuid", Collections.<byte[]>emptyList(), 0);
				will(returnValue(Arrays.asList("1".getBytes())));
		}});
		assertEquals(1, dut.genuid());
	}

	@Test public void genuidFailure() {
		mockery.checking(new Expectations() {{
			one(db).misc("genuid", Collections.<byte[]>emptyList(), 0);
				will(returnValue(null));
		}});
		assertEquals(-1, dut.genuid());
	}
	
	@Test public void searchSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("search")), with(new MiscListMatcher(Collections.<byte[]>emptyList())), with(equal(RDB.MONOULOG)));
				will(returnValue(Arrays.asList("ckey".getBytes(), "\0\0[[HINT]]\nhint".getBytes())));
		}});
		TableQuery query = new TableQuery();
		assertEquals(Arrays.asList("ckey"), dut.search(query));
		assertEquals("hint", query.hint);
	}
	
	@Test public void searchOutSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("search")), with(new MiscListMatcher(Arrays.asList("out".getBytes()))), with(equal(RDB.MONOULOG)));
				will(returnValue(Arrays.asList("\0\0[[HINT]]\nhint".getBytes())));
		}});
		TableQuery query = new TableQuery();
		assertTrue(dut.searchOut(query));
		assertEquals("hint", query.hint);
	}

	@Test public void searchCountSuccess() {
		mockery.checking(new Expectations() {{
			one(db).misc(with(equal("search")), with(new MiscListMatcher(Arrays.asList("count".getBytes()))), with(equal(RDB.MONOULOG)));
				will(returnValue(Arrays.asList("42".getBytes(), "\0\0[[HINT]]\nhint".getBytes())));
		}});
		TableQuery query = new TableQuery();
		assertEquals(42, dut.searchCount(query));
		assertEquals("hint", query.hint);
	}
}
