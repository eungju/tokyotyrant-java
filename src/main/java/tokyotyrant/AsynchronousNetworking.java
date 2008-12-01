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
			selector.wakeup().close();
		} catch (IOException e) {
			log.error("Error while closing selector", e);
		}
	}
	
	public void run() {		
		while (running) {
			try {
				log.debug("Selecting...");
				int n = selector.select();
				if (n == 0 || !selector.isOpen()) {
					continue;
				}
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> i = selectedKeys.iterator();
				while (i.hasNext()) {
					SelectionKey key = i.next();
					SocketChannel channel = (SocketChannel)key.channel();
					AsynchronousNode node = (AsynchronousNode)key.attachment();
					
					if (!key.isValid() || !channel.isOpen()) {
						continue;
					} else if (key.isConnectable()) {
						log.debug("Ready to connect");
						
						doConnect(channel, node);
					} else if (key.isReadable()) {
						log.debug("Ready to read");
						
						doRead(channel, node);
					}
					i.remove();
				}
			} catch (Exception e) {
				log.error("Error while processing network communication", e);
			}
		}
	}

	void doConnect(SocketChannel channel, AsynchronousNode node) {
		try {
			node.doConnect();
		} catch (IOException e) {
			log.error("Error while connecting to " + node, e);
			node.reconnect();
		}
	}
	
	void doRead(SocketChannel channel, AsynchronousNode node) {
		try {
			node.doRead();
		} catch (IOException e) {
			log.error("Error while reading from " + node, e);
			node.reconnect();
		}
	}
	
	public void send(Command<?> command) {
		try {
			node.send(command);
		} catch (IOException e) {
			log.error("Error while sending command " + command + " to " + node, e);
			node.reconnect();
		}
	}
}
