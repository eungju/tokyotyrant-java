package tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;


public class AsynchronousNode implements TokyoTyrantNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private SocketAddress address;
	private int reconnectAttempt = 1;

	private Selector selector;
	private SocketChannel channel;
	private SelectionKey selectionKey;
	private BlockingQueue<Command<?>> writingQueue = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private BlockingQueue<Command<?>> readingQueue = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer readBuffer;
	
	public AsynchronousNode(SocketAddress address, Selector selector) {
		this.address = address;
		this.selector = selector;
	}
		
	public void start() {
		connect();
	}

	public void stop() {
		disconnect();
	}

	public void send(Command<?> command) {
		writingQueue.add(command);
		fixupOperations();
		selector.wakeup();
	}

	void sendRequest(Command<?> command) throws IOException {
		ByteBuffer buffer = command.encode();
		int written = 0;
		do {
			int n = channel.write(buffer);
			written += n;
		} while (written != buffer.limit());
		logger.debug("Sent message " + buffer);
	}

	public boolean isActive() {
		return reconnectAttempt == 0 && channel != null && channel.isConnected();
	}

	public void connect() {
		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(address);
			selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, this);
		} catch (IOException e) {
			logger.error("Cannot open connection to " + address, e);
		}
	}

	public void disconnect() {
		readingQueue.clear();
		try {
			channel.close();
		} catch (IOException e) {
			logger.error("Error while closing connection to " + address, e);
		}
	}

	public void reconnect() {
		logger.info("Reconnecting to " + address);
		reconnectAttempt++;
		disconnect();
		connect();
	}

	void fixupOperations() {
		int ops = 0;
		if (channel.isConnected()) {
			if (!readingQueue.isEmpty()) {
				ops |= SelectionKey.OP_READ;
			}
			if (!writingQueue.isEmpty()) {
				ops |= SelectionKey.OP_WRITE;
			}
		} else {
			ops = SelectionKey.OP_CONNECT;
		}
		selectionKey.interestOps(ops);
	}

	public void doConnect() throws IOException {
		channel.finishConnect();
		fixupOperations();
		reconnectAttempt = 0;
	}

	private static final int FRAGMENT_CAPACITY = 2 * 1024;
	
	public void doWrite() throws IOException {
		Command<?> command = writingQueue.peek();
		try {
			sendRequest(command);
			writingQueue.remove();
			command.reading();
			readingQueue.add(command);
		} catch (IOException exception) {
			command.error(exception);
			throw exception;
		}
		fixupOperations();
	}
	
	public void doRead() throws IOException {
		Command<?> command = readingQueue.peek();
		try {
			ByteBuffer fragment = ByteBuffer.allocate(FRAGMENT_CAPACITY);
			if (channel.read(fragment) == -1) {
				throw new IOException("Channel " + channel + " is closed");
			}
			fragment.flip();
			logger.debug("Received fragment {}", fragment);
			received(fragment, command);
		} catch (IOException exception) {
			command.error(exception);
			throw exception;
		}
		fixupOperations();
	}
	
	void received(ByteBuffer fragment, Command<?> command) {
		if (readBuffer == null) {
			readBuffer = ByteBuffer.allocate(FRAGMENT_CAPACITY);
		}
		
		readBuffer = BufferHelper.accumulateBuffer(readBuffer, fragment);
		int pos = readBuffer.position();
		readBuffer.flip();
		if (command.decode(readBuffer)) {
			logger.debug("Received message " + readBuffer);
			command.complete();
			readingQueue.remove();
			
			if (readBuffer.hasRemaining()) {
				ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.remaining() + FRAGMENT_CAPACITY);
				newBuffer.put(readBuffer);
				readBuffer = newBuffer;
			} else {
				readBuffer = null;
			}
		} else {
			readBuffer.position(pos);
		}
	}
	
	public String toString() {
		return "AsynchronousNode[" + address.toString() + "]";
	}
}
