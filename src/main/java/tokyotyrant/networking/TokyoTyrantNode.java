package tokyotyrant.networking;

import java.io.IOException;

import tokyotyrant.protocol.Command;


public interface TokyoTyrantNode {
	void start();
	
	void stop();

	void send(Command<?> command) throws IOException;
	
	boolean isActive();
	
	void connect();
	
	void disconnect();
	
	void reconnect();
}
