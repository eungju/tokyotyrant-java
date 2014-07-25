package tokyotyrant;

import org.junit.Ignore;
import org.junit.Test;
import tokyotyrant.networking.NodeAddress;
import tokyotyrant.transcoder.ByteArrayTranscoder;
import tokyotyrant.transcoder.StringTranscoder;

import static org.junit.Assert.*;

public class RegressionTest {
    @Test
    public void shouldNotOmitRequestsWithoutException() throws Exception {
        final MRDB db = new MRDB();
        db.open(NodeAddress.addresses("tcp://localhost:1978"));
        db.setKeyTranscoder(new StringTranscoder());
        db.setValueTranscoder(new ByteArrayTranscoder());
        db.vanish().get();
        int N = 100000;
        byte[] value = new byte[1024];
        for (int i = 0; i < N; i++) {
            String x = String.format("%10d", i);
            db.put(x, value).get();
        }
        assertEquals(N, (long) db.rnum().get());
        db.close();
    }
}
