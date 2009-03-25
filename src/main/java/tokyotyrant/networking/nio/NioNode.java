package tokyotyrant.networking.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.networking.NodeAddress;
import tokyotyrant.networking.ServerNode;
import tokyotyrant.protocol.Command;

public class NioNode implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private NodeAddress address;
	private int bufferCapacity = 8 * 1024;

	Selector selector;
	SocketChannel channel;
	SelectionKey selectionKey;
	int reconnecting = 0;
	
	BlockingQueue<Command<?>> writingCommands = new LinkedBlockingQueue<Command<?>>();
	ChannelBuffer outgoingBuffer = ChannelBuffers.dynamicBuffer(bufferCapacity);

	BlockingQueue<Command<?>> readingCommands = new LinkedBlockingQueue<Command<?>>();
	ChannelBuffer incomingBuffer = ChannelBuffers.dynamicBuffer(bufferCapacity);
	
	public NioNode(Selector selector) {
		this.selector = selector;
	}
	
	public void initialize(NodeAddress address) {
		this.address = address;
	}
	
	public NodeAddress getAddress() {
		return address;
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
			channel.socket().setTcpNoDelay(true);
			channel.socket().setKeepAlive(true);
			channel.connect(address.socketAddress());
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

	public void fixupInterests() {
		if (selectionKey == null || !selectionKey.isValid()) {
			return;
		}
		
		int ops = 0;
		if (channel.isConnected()) {
			if (!readingCommands.isEmpty()) {
				ops |= SelectionKey.OP_READ;
			}
			if (outgoingBuffer.readable() || !writingCommands.isEmpty()) {
				ops |= SelectionKey.OP_WRITE;
			}
		} else {
			ops = SelectionKey.OP_CONNECT;
		}
		selectionKey.interestOps(ops);
	}
	
	public void handleConnect() throws Exception {
		if (!channel.finishConnect()) {
			throw new IllegalStateException("Connection is not established");
		}
		reconnecting = 0;
	}
	
	public void handleWrite() throws Exception {
		fillOutgoingBuffer();
		consumeOutgoingBuffer();
	}
	
	void fillOutgoingBuffer() throws Exception {
		while (!writingCommands.isEmpty()) {
			Command<?> command = writingCommands.peek();
			try {
				command.encode(outgoingBuffer);
				Command<?> _removed = writingCommands.remove();
				assert _removed == command;
				command.reading();
				readingCommands.add(command);
			} catch (Exception exception) {
				command.error(exception);
				throw new Exception("Error while sending " + command, exception);
			}
		}
	}
	
	void consumeOutgoingBuffer() throws IOException {
		ByteBuffer chunk = outgoingBuffer.toByteBuffer();
		int n = channel.write(chunk);
		outgoingBuffer.skipBytes(n);
	}

	public void handleRead() throws Exception {
		fillIncomingBuffer();
		consumeIncomingBuffer();
	}
	
	void fillIncomingBuffer() throws IOException {
		ByteBuffer chunk = ByteBuffer.allocate(bufferCapacity);
		int n = channel.read(chunk);
		if (n == -1) {
			throw new IOException("Channel " + channel + " is closed");
		}
		chunk.flip();
		incomingBuffer.writeBytes(chunk);
	}
	
	void consumeIncomingBuffer() throws Exception {
		while (!readingCommands.isEmpty()) {
			Command<?> command = readingCommands.peek();
			try {
				incomingBuffer.markReaderIndex();
				if (!command.decode(incomingBuffer)) {
					incomingBuffer.resetReaderIndex();
					break;
				}
				command.complete();
				Command<?> _removed = readingCommands.remove();
				assert _removed == command;
			} catch (Exception exception) {
				command.error(exception);
				throw new Exception("Error while receiving " + command, exception);
			}
		}
	}
	
	public String toString() {
		return getClass().getName() + "[" + address.toString() + "]";
	}
}
