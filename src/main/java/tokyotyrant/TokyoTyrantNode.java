package tokyotyrant;


public interface TokyoTyrantNode {
	void start();
	
	void stop();

	void send(Command<?> command);
	
	boolean isActive();
	
	void connect();
	
	void disconnect();
	
	void reconnect();
}
