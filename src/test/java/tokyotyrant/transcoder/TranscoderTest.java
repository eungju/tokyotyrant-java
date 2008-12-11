package tokyotyrant.transcoder;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

public class TranscoderTest {
	protected Transcoder dut;
	
	@Test(expected=NullPointerException.class)
	public void shouldNotEncodeNull() {
		System.err.println(ArrayUtils.toString(dut.encode(null)));
	}
	
	@Test(expected=NullPointerException.class)
	public void shouldNotDecodeNull() {
		System.err.println(dut.decode(null));
	}
}