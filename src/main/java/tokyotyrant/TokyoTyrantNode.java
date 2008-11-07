package tokyotyrant;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface TokyoTyrantNode {
	boolean isActive();
	
	void connect();
	
	void disconnect();
	
	void reconnect();
	
	void write(ByteBuffer buffer) throws IOException;
	
	int read(ByteBuffer buffer) throws IOException;
}
