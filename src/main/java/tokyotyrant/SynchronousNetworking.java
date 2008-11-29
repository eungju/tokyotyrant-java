package tokyotyrant;

import java.net.SocketAddress;

public class SynchronousNetworking implements Networking {
	/** In millisecond */
	private int socketTimeout = 1000;
	private TokyoTyrantNode node;

	public SynchronousNetworking(SocketAddress serverAddress) {
		node = new SynchronousNode(serverAddress, socketTimeout);
	}

	public void start() {
		node.start();
	}

	public void stop() {
		node.stop();
	}

	public void send(Command<?> command) {
		node.send(command);
	}
}
