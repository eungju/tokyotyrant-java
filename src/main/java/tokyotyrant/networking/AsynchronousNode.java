package tokyotyrant.networking;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
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

public class AsynchronousNode implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private URI address;
	private boolean readOnly;

	private Selector selector;
	private SocketChannel channel;
	private SelectionKey selectionKey;
	private int reconnecting = 0;
	
	private BlockingQueue<Command<?>> writingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer writingBuffer = null;
	private BlockingQueue<Command<?>> readingCommands = new ArrayBlockingQueue<Command<?>>(16 * 1024);
	private ByteBuffer readingBuffer = null;
	
	public AsynchronousNode(URI address, Selector selector) {
		if (!"tcp".equals(address.getScheme())) {
			throw new IllegalArgumentException("Only support TCP binary protocol");
		}
		this.address = address;
		this.selector = selector;
		readOnly = this.address.getQuery().indexOf("readOnly=true") >= 0;
	}
	
	public URI getAddress() {
		return address;
	}
	
	public boolean isReadOnly() {
		return readOnly;
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
	
	SocketAddress getSocketAddress(URI uri) {
		return new InetSocketAddress(address.getHost(), address.getPort());
	}

	public boolean connect() {
		logger.info("Connect " + address);
		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(getSocketAddress(address));
			selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, this);
			return true;
		} catch (IOException e) {
			logger.error("Error while opening connection to " + address, e);
			return false;
		}
	}

	public void disconnect() {
		logger.info("Diconnect " + address);
		writingBuffer = null;
		readingBuffer = null;
		readingCommands.clear();
		try {
			selectionKey.cancel();
			channel.close();
		} catch (IOException e) {
			logger.error("Error while closing connection to " + address, e);
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

	public void doConnect() throws IOException {
		channel.finishConnect();
		reconnecting = 0;
		fixupOperations();
	}

	private static final int FRAGMENT_CAPACITY = 2 * 1024;
	
	public void doWrite() throws IOException {
		Command<?> command = writingCommands.peek();
		if (writingBuffer == null) {
			writingBuffer = command.encode();
		}
		try {
			channel.write(writingBuffer);
			if (!writingBuffer.hasRemaining()) {
				writingBuffer = null;
				Command<?> _removed = writingCommands.remove();
				assert _removed == command;
				command.reading();
				readingCommands.add(command);
			}
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
			Command<?> _removed = readingCommands.remove();
			assert _removed == command;
			
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
