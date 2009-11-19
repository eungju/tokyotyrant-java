package tokyotyrant;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class TableQueryTest {
	@Test public void condition() {
		assertEquals(Arrays.asList(new TableQuery.Condition("name", TableQuery.QCSTREQ, "expr")), new TableQuery().condition("name", TableQuery.QCSTREQ, "expr").conditions);
	}
	
	@Test public void order() {
		assertEquals(new TableQuery.Order("name", TableQuery.QONUMASC), new TableQuery().order("name", TableQuery.QONUMASC).order);
	}

	@Test public void limit() {
		assertEquals(new TableQuery.Limit(10, 0), new TableQuery().limit(10, 0).limit);
	}
}
