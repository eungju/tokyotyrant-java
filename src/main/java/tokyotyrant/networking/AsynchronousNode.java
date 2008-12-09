package tokyotyrant.networking;

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
import tokyotyrant.protocol.Command;


public class AsynchronousNode implements TokyoTyrantNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private SocketAddress address;
	private int reconnectAttempt = 1;

	private Selector selector;
	private SocketChannel channel;
	private SelectionKey selectionKey;
	private BlockingQueue<Command<?>> writingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private BlockingQueue<Command<?>> readingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer readingBuffer;
	
	public AsynchronousNode(SocketAddress address, Selector selector) {
		this.address = address;
		this.selector = selector;
	}
		
	public void send(Command<?> command) {
		writingCommands.add(command);
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
	
	public int getReconnectAttempt() {
		return reconnectAttempt;
	}

	public void connect() throws IOException {
		channel = SocketChannel.open();
		channel.configureBlocking(false);
		channel.connect(address);
		selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, this);
	}

	public void disconnect() {
		readingCommands.clear();
		readingBuffer = null;
		try {
			selectionKey.cancel();
			channel.close();
		} catch (IOException e) {
			logger.error("Error while closing connection to " + address, e);
		}
	}

	public void reconnecting() {
		logger.info("Reconnecting to " + address);
		reconnectAttempt++;
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

	public void doConnect() throws IOException {
		channel.finishConnect();
		fixupOperations();
		reconnectAttempt = 0;
	}

	private static final int FRAGMENT_CAPACITY = 2 * 1024;
	
	public void doWrite() throws IOException {
		Command<?> command = writingCommands.peek();
		try {
			sendRequest(command);
			writingCommands.remove();
			command.reading();
			readingCommands.add(command);
		} catch (IOException exception) {
			command.error(exception);
			throw exception;
		}
		fixupOperations();
	}
	
	public void doRead() throws IOException {
		Command<?> command = readingCommands.peek();
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
		if (readingBuffer == null) {
			readingBuffer = ByteBuffer.allocate(FRAGMENT_CAPACITY);
		}
		
		readingBuffer = BufferHelper.accumulateBuffer(readingBuffer, fragment);
		int pos = readingBuffer.position();
		readingBuffer.flip();
		if (command.decode(readingBuffer)) {
			logger.debug("Received message " + readingBuffer);
			command.complete();
			readingCommands.remove();
			
			if (readingBuffer.hasRemaining()) {
				ByteBuffer newBuffer = ByteBuffer.allocate(readingBuffer.remaining() + FRAGMENT_CAPACITY);
				newBuffer.put(readingBuffer);
				readingBuffer = newBuffer;
			} else {
				readingBuffer = null;
			}
		} else {
			readingBuffer.position(pos);
		}
	}
	
	public String toString() {
		return "AsynchronousNode[" + address.toString() + "]";
	}
}
