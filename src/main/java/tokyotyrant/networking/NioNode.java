package tokyotyrant.networking;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;
import tokyotyrant.helper.UriHelper;
import tokyotyrant.protocol.Command;

public class NioNode implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private URI address;
	private SocketAddress socketAddress;
	private Map<String, String> parameters;
	private int bufferCapacity = 4 * 1024;

	private Selector selector;
	private SocketChannel channel;
	private SelectionKey selectionKey;
	private int reconnecting = 0;
	
	private BlockingQueue<Command<?>> writingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer writingBuffer = null;
	private BlockingQueue<Command<?>> readingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer readingBuffer = ByteBuffer.allocate(bufferCapacity);
	
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
		fixupOperations();
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
			writingBuffer = null;
			readingBuffer.clear();
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

	void fixupOperations() {
		int ops = 0;
		if (channel.isConnected()) {
			if (!readingCommands.isEmpty()) {
				ops |= SelectionKey.OP_READ;
			}
			if (!writingCommands.isEmpty()) {
				ops |= SelectionKey.OP_WRITE;
			}
		} else {
			ops = SelectionKey.OP_CONNECT;
		}
		selectionKey.interestOps(ops);
	}

	public void handleConnect() throws IOException {
		channel.finishConnect();
		reconnecting = 0;
		fixupOperations();
	}

	public void handleWrite() throws Exception {
		while (!writingCommands.isEmpty()) {
			Command<?> command = writingCommands.peek();
			if (writingBuffer == null) {
				writingBuffer = command.encode();
			}
			try {
				int n = channel.write(writingBuffer);
				if (!writingBuffer.hasRemaining()) {
					writingBuffer = null;
					Command<?> _removed = writingCommands.remove();
					assert _removed == command;
					command.reading();
					readingCommands.add(command);
				}
				if (n == 0) {
					break;
				}
			} catch (IOException exception) {
				command.error(exception);
				throw exception;
			} finally {
				fixupOperations();
			}
		}
	}

	public void handleRead() throws Exception {
		//fill the reading buffer
		while (true) {
			int n = channel.read(readingBuffer);
			if (n == 0) {
				break;
			} else if (n == -1) {
				throw new IOException("Channel " + channel + " is closed");
			}
			logger.debug("{} bytes received", n);

			//expand if necessary
			if (readingBuffer.remaining() < bufferCapacity) {
				readingBuffer = BufferHelper.expand(readingBuffer);
			}
		}

		//decode all commands in reading buffer
		readingBuffer.flip();
		while (!readingCommands.isEmpty()) {
			Command<?> command = readingCommands.peek();
			try {
				int pos = readingBuffer.position();
				logger.debug("Try to decode {}", readingBuffer);
				if (command.decode(readingBuffer)) {
					logger.debug("Received response of {}", command);
					command.complete();
					Command<?> _removed = readingCommands.remove();
					assert _removed == command;
				} else {
					//TODO: need to shrink aggressively?
					if (readingBuffer.hasRemaining()) {
						readingBuffer.position(pos);
						ByteBuffer newReadingBuffer = ByteBuffer.allocate(readingBuffer.capacity());
						newReadingBuffer.put(readingBuffer);
						readingBuffer = newReadingBuffer;
					} else {
						readingBuffer.clear();
					}
					break;
				}
			} catch (Exception exception) {
				command.error(exception);
				throw new Exception("Error while reading response of command " + command, exception);
			} finally {
				fixupOperations();
			}
		}
	}
	
	public String toString() {
		return getClass().getName() + "[" + address.toString() + "]";
	}
}
