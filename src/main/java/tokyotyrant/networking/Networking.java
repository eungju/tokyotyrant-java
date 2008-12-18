package tokyotyrant.networking;

import java.net.SocketAddress;

import tokyotyrant.protocol.Command;

public interface Networking {
	void setAddresses(SocketAddress[] addresses);
	NodeLocator getNodeLocator();
	void start();
	void stop();
	void send(Command<?> command);
	void send(ServerNode node, Command<?> command);
}
