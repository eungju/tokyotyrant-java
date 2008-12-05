package tokyotyrant.networking;

import tokyotyrant.protocol.Command;

public interface Networking {
	void start();
	void stop();
	void send(Command<?> command);
}
