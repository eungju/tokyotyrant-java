package tokyotyrant.networking;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.protocol.Command;

public class AsynchronousNetworking implements Networking, Runnable {
	private final Logger log = LoggerFactory.getLogger(getClass());  
	private Selector selector;
	private Thread ioThread;
	private boolean running;

	private List<AsynchronousNode> nodes;
	private ReconnectQueue reconnectQueue = new ReconnectQueue();

	public AsynchronousNetworking(SocketAddress... addresses) throws IOException {
		selector = Selector.open();
		ioThread = new Thread(this);
		
		nodes = new ArrayList<AsynchronousNode>();
		for (SocketAddress each : addresses) {
			nodes.add(new AsynchronousNode(each, selector));
		}
	}

	public void start() {
		running = true;
		ioThread.start();

		for (AsynchronousNode each : nodes) {
			try {
				each.connect();
			} catch (IOException e) {
				log.error("Cannot open connection to " + each, e);
				reconnectQueue.push(each);
			}
		}
	}
	
	public void stop() {
		for (AsynchronousNode each : nodes) {
			each.disconnect();
		}

		running = false;
		try {
			selector.wakeup().close();
		} catch (IOException e) {
			log.error("Error while closing selector", e);
		}
	}
	
	public void send(Command<?> command) {
		AsynchronousNode selected = nodes.get(0);
		for (AsynchronousNode each : nodes) {
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		selected.send(command);
	}

	public void run() {		
		while (running) {
			try {
				handleIO();
			} catch (Exception e) {
				log.error("Error while handling IO", e);
			}
		}
	}
	
	void handleIO() throws IOException {
		log.debug("Selecting...");
		int n = selector.select(reconnectQueue.getTimeToNextAttempt());
		log.debug("{} keys are selected", n);
		
		Set<SelectionKey> selectedKeys = selector.selectedKeys();
		Iterator<SelectionKey> i = selectedKeys.iterator();
		while (i.hasNext()) {
			SelectionKey key = i.next();
			i.remove();
			handleChannelIO(key);
		}
		
		reconnectQueue.reconnect();
	}
	
	void handleChannelIO(SelectionKey key) {
		AsynchronousNode node = (AsynchronousNode)key.attachment();
		try {
			if (key.isConnectable()) {
				log.debug("Ready to connect to {}", node);
				node.doConnect();
			} else {
				if (key.isReadable()) {
					log.debug("Ready to read from {}", node);
					node.doRead();
				}
				if (key.isWritable()) {
					log.debug("Ready to write to {}", node);
					node.doWrite();
				}
			}
		} catch (Exception e) {
			log.error("Error while handling IO on " + node, e);
			reconnectQueue.push(node);
		}
	}
}
