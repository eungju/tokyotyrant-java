package org.zact.tokyotyrant;

import java.io.IOException;

public interface Networking {
	void start();
	void stop();
	void execute(Command command) throws IOException;
}
