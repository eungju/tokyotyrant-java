package tokyotyrant.networking;

import java.io.IOException;

import tokyotyrant.protocol.Command;

public interface TokyoTyrantNode {
	void send(Command<?> command) throws IOException;

	boolean isActive();

	int getReconnectAttempt();

	/**
	 * Open connection.
	 */
	void connect() throws IOException;

	/**
	 * Disconnect connection.
	 */
	void disconnect();

	/**
	 * {@link Networking} notice the reconnection to this node.
	 */
	void reconnecting();
}
