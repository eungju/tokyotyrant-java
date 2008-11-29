package tokyotyrant;

public interface Networking {
	void start();
	void stop();
	void send(Command<?> command);
}
