package tokyotyrant.networking;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsynchronousNetworking extends AbstractNetworking implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(getClass());  
	private Selector selector;
	private Thread ioThread;
	private boolean running;

	public AsynchronousNetworking() throws IOException {
		super(new ReplicationNodeLocator());
		selector = Selector.open();
		ioThread = new Thread(this);
	}
	
	public void setAddresses(SocketAddress[] addresses) {
		TokyoTyrantNode[] nodes = new TokyoTyrantNode[addresses.length];
		for (int i = 0; i < addresses.length; i++) {
			nodes[i] = new AsynchronousNode(addresses[i], selector);
		}
		nodeLocator.setNodes((List<TokyoTyrantNode>) Arrays.asList(nodes));
	}

	public void start() {
		running = true;
		ioThread.start();

		connectAllNodes();
	}
	
	public void stop() {
		disconnectAllNodes();

		running = false;
		try {
			selector.wakeup().close();
		} catch (IOException e) {
			logger.error("Error while closing selector", e);
		}
	}

	public void run() {		
		while (running) {
			try {
				handleIO();
			} catch (Exception e) {
				logger.error("Error while handling IO", e);
			}
		}
	}
	
	void handleIO() throws IOException {
		logger.debug("Selecting...");
		int n = selector.select(reconnectQueue.getTimeToNextAttempt());
		logger.debug("{} keys are selected", n);
		if (!running) {
			logger.info("Stopped. So will not handle IO. {} keys will be ignored", n);
			return;
		}
		
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
				logger.debug("Ready to connect to {}", node);
				node.doConnect();
			} else {
				if (key.isReadable()) {
					logger.debug("Ready to read from {}", node);
					node.doRead();
				}
				if (key.isWritable()) {
					logger.debug("Ready to write to {}", node);
					node.doWrite();
				}
			}
		} catch (Exception e) {
			logger.error("Error while handling IO on " + node, e);
			reconnectQueue.push(node);
		}
	}
}
