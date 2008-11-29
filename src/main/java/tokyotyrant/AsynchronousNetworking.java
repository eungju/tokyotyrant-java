package tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsynchronousNetworking implements Networking, Runnable {
	private final Logger log = LoggerFactory.getLogger(getClass());  
	private Selector selector;
	private Thread ioThread;
	private boolean running;

	private AsynchronousNode node;
	
	public AsynchronousNetworking(SocketAddress serverAddress) throws IOException {
		selector = Selector.open();
		ioThread = new Thread(this);
		node = new AsynchronousNode(serverAddress, selector);
	}
	
	public void start() {
		node.start();

		running = true;
		ioThread.start();
	}
	
	public void stop() {
		running = false;

		node.stop();

		try {
			selector.close();
		} catch (IOException e) {
			log.error("Error while closing selector", e);
		}
	}
	
	public void run() {		
		while (running) {
			try {
				log.debug("Selecting...");
				selector.select();
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> i = selectedKeys.iterator();
				while (i.hasNext()) {
					SelectionKey key = i.next();
					if (key.isConnectable()) {
						log.debug("Ready to connect");
						((SocketChannel)key.channel()).finishConnect();
						key.channel().register(selector, SelectionKey.OP_WRITE);
						key.channel().register(selector, SelectionKey.OP_READ);
						node.connected();
					}
					if (key.isWritable()) {
						log.debug("Ready to write");
					}
					if (key.isReadable()) {
						log.debug("Ready to read");
						node.read();
					}
					i.remove();
				}
			} catch (Exception e) {
				log.error("Error while processing network communication", e);
			}
		}
	}
	
	public void send(Command<?> command) {
		node.send(command);
	}
}
