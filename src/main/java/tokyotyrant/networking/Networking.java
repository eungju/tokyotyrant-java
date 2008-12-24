package tokyotyrant.networking;

import java.net.URI;

import tokyotyrant.protocol.Command;

public interface Networking {
	void setAddresses(URI[] addresses);
	
	void start() throws Exception;
	
	void stop();
	
	void send(Command<?> command);
	
	void send(ServerNode node, Command<?> command);
	
	NodeLocator getNodeLocator();
}
