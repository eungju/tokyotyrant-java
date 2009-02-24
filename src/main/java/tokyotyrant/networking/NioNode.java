package tokyotyrant.networking;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.UriHelper;
import tokyotyrant.protocol.Command;

public class NioNode implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private URI address;
	private SocketAddress socketAddress;
	private Map<String, String> parameters;
	private int bufferCapacity = 8 * 1024;

	Selector selector;
	SocketChannel channel;
	SelectionKey selectionKey;
	int reconnecting = 0;
	
	BlockingQueue<Command<?>> writingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	OutgoingBuffer outgoingBuffer = new OutgoingBuffer(bufferCapacity);

	BlockingQueue<Command<?>> readingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	IncomingBuffer incomingBuffer = new IncomingBuffer(bufferCapacity);
	
	public NioNode(Selector selector) {
		this.selector = selector;
	}
	
	public void initialize(URI address) {
		if (!"tcp".equals(address.getScheme())) {
			throw new IllegalArgumentException("Only support Tokyo Tyrant protocol");
		}
		this.address = address;
		
		socketAddress = UriHelper.getSocketAddress(address);
		parameters = UriHelper.getParameters(address);
	}
	
	public URI getAddress() {
		return address;
	}
	
	public boolean isReadOnly() {
		return parameters.containsKey("readOnly") && "true".equals(parameters.get("readOnly"));
	}
		
	public void send(Command<?> command) {
		writingCommands.add(command);
		selector.wakeup();
	}

	public boolean isActive() {
		return reconnecting == 0 && channel != null && channel.isConnected();
	}
	
	public int getReconnectAttempt() {
		return reconnecting;
	}
	
	public boolean connect() {
		logger.info("Connect " + address);
		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(socketAddress);
			selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, this);
			return true;
		} catch (IOException e) {
			logger.error("Error while opening connection to " + address, e);
			return false;
		}
	}

	public void disconnect() {
		logger.info("Disconnect " + address);
		try {
			selectionKey.cancel();
			channel.close();
		} catch (IOException e) {
			logger.error("Error while closing connection to " + address, e);
		} finally {
			outgoingBuffer.clear();
			incomingBuffer.clear();
			for (Iterator<Command<?>> i = readingCommands.iterator(); i.hasNext(); ) {
				Command<?> each = i.next();
				each.cancel();
				i.remove();
			}
			assert readingCommands.isEmpty();
		}
	}

	public void reconnecting() {
		logger.info("Reconnecting " + address);
		reconnecting++;
	}

	public void fixupOperations() {
		int ops = 0;
		if (channel.isConnected()) {
			if (!readingCommands.isEmpty()) {
				ops |= SelectionKey.OP_READ;
			}
			if (!outgoingBuffer.isEmpty() || !writingCommands.isEmpty()) {
				ops |= SelectionKey.OP_WRITE;
			}
		} else {
			ops = SelectionKey.OP_CONNECT;
		}
		selectionKey.interestOps(ops);
	}
	
	public void handleConnect() throws IOException {
		if (!channel.finishConnect()) {
			throw new IllegalStateException("Connection is not established");
		}
		reconnecting = 0;
	}
	
	public void handleWrite() throws IOException {
		//fill the buffer
		if (outgoingBuffer.isEmpty()) {
			while (!writingCommands.isEmpty() && outgoingBuffer.isHungry()) {
				Command<?> command = writingCommands.peek();
				try {
					outgoingBuffer.fill(command);
					Command<?> _removed = writingCommands.remove();
					assert _removed == command;
					command.reading();
					readingCommands.add(command);
				} catch (Exception exception) {
					command.error(exception);
					throw new IOException("Cannot write " + command, exception);
				}
			}
		}
		
		//consume the buffer
		outgoingBuffer.consume(channel);
	}

	public void handleRead() throws IOException {
		//fill the buffer
		incomingBuffer.fill(channel);

		//consume the buffer
		while (!readingCommands.isEmpty()) {
			Command<?> command = readingCommands.peek();
			try {
				if (incomingBuffer.consume(command)) {
					logger.debug("Received response of {}", command);
					command.complete();
					Command<?> _removed = readingCommands.remove();
					assert _removed == command;
				} else {
					break;
				}
			} catch (Exception exception) {
				command.error(exception);
				throw new IOException("Cannot read " + command, exception);
			}
		}
	}
	
	public String toString() {
		return getClass().getName() + "[" + address.toString() + "]";
	}
}
