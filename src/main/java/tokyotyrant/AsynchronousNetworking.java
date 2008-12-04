package tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsynchronousNetworking implements Networking, Runnable {
	private final Logger log = LoggerFactory.getLogger(getClass());  
	private Selector selector;
	private Thread ioThread;
	private boolean running;

	private List<AsynchronousNode> nodes;
	
	public AsynchronousNetworking(SocketAddress address) throws IOException {
		this(Arrays.asList(address));
	}

	public AsynchronousNetworking(List<SocketAddress> addresses) throws IOException {
		selector = Selector.open();
		ioThread = new Thread(this);
		
		nodes = new ArrayList<AsynchronousNode>();
		for (SocketAddress each : addresses) {
			nodes.add(new AsynchronousNode(each, selector));
		}
	}

	public void start() {
		for (AsynchronousNode each : nodes) {
			each.start();
		}

		running = true;
		ioThread.start();
	}
	
	public void stop() {
		for (AsynchronousNode each : nodes) {
			each.stop();
		}

		running = false;
		try {
			selector.wakeup().close();
		} catch (IOException e) {
			log.error("Error while closing selector", e);
		}
	}
	
	public void send(Command<?> command) {
		AsynchronousNode node = nodes.get(0);
		node.send(command);
	}

	public void run() {		
		while (running) {
			try {
				log.debug("Selecting...");
				int n = selector.select();
				log.debug("{} keys are selected", n);
				if (!selector.isOpen()) {
					continue;
				}
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> i = selectedKeys.iterator();
				while (i.hasNext()) {
					SelectionKey key = i.next();
					i.remove();
					SocketChannel channel = (SocketChannel)key.channel();
					AsynchronousNode node = (AsynchronousNode)key.attachment();
					
					if (!key.isValid()) {
						log.warn("SelectionKey {} is not valid", key);
					} else if (!channel.isOpen()) {
						log.warn("Channel {} is not open", channel);
					} else if (key.isConnectable()) {
						log.debug("Ready to connect to {}", node);
						
						doConnect(node);
					} else {
						if (key.isReadable()) {
							log.debug("Ready to read from {}", node);
						 
							doRead(node);
						}
						if (key.isWritable()) {
							log.debug("Ready to write to {}", node);
							
							doWrite(node);
						}
					}
				}
			} catch (Exception e) {
				log.error("Error while processing network communication", e);
			}
		}
	}

	void doConnect(AsynchronousNode node) {
		try {
			node.doConnect();
		} catch (IOException e) {
			log.error("Error while connecting to " + node, e);
			node.reconnect();
		}
	}
	
	void doWrite(AsynchronousNode node) {
		try {
			node.doWrite();
		} catch (IOException e) {
			log.error("Error while writing to " + node, e);
			node.reconnect();
		}
	}

	void doRead(AsynchronousNode node) {
		try {
			node.doRead();
		} catch (IOException e) {
			log.error("Error while reading from " + node, e);
			node.reconnect();
		}
	}
}
