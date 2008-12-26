package tokyotyrant.networking;

import java.net.URI;

import tokyotyrant.protocol.Command;

public interface ServerNode {
	void initialize(URI address);
	
	URI getAddress();
	
	void send(Command<?> command);

	boolean isActive();

	int getReconnectAttempt();

	/**
	 * Open connection.
	 */
	boolean connect();

	/**
	 * Disconnect connection.
	 */
	void disconnect();

	/**
	 * {@link Networking} notice the reconnection to this node.
	 */
	void reconnecting();
}
