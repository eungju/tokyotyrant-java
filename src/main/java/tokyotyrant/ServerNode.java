package tokyotyrant;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ServerNode {
	void connect();
	
	void close();
	
	void reconnect();
	
	void write(ByteBuffer buffer) throws IOException;
	
	int read(ByteBuffer buffer) throws IOException;
}
