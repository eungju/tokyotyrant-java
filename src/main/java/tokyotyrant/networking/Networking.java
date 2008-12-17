package tokyotyrant.networking;

import java.net.SocketAddress;

import tokyotyrant.protocol.Command;

public interface Networking {
	void setAddresses(SocketAddress[] addresses);
	void start();
	void stop();
	void send(Command<?> command);
}
