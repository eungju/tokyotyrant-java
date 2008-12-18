package tokyotyrant.networking;

import java.net.SocketAddress;

import tokyotyrant.protocol.Command;

public interface ServerNode {
	SocketAddress getSocketAddress();
	
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
