package tokyotyrant;

import java.io.IOException;


public interface TokyoTyrantNode {
	void start();
	
	void stop();

	void send(Command<?> command) throws IOException;
	
	boolean isActive();
	
	void connect();
	
	void disconnect();
	
	void reconnect();
}
