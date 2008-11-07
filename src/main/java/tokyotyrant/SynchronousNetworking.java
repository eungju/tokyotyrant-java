package tokyotyrant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;

public class SynchronousNetworking implements Networking {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	/** In millisecond */
	private int timeout = 1000;
	private TokyoTyrantNode node;
	private Lock lock = new ReentrantLock();

	public SynchronousNetworking(SocketAddress serverAddress) {
		node = new SynchronousServerNode(serverAddress, timeout);
	}

	public void start() {
		node.connect();
	}

	public void stop() {
		node.disconnect();
	}

	public void execute(Command command) throws IOException {
		try {
			if (!lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
				throw new IOException("Unable to aquire access to the node");
			}
		} catch (InterruptedException e) {
			logger.error("Lock acquisition is interrupted", e);
			return;
		}
		
		try {
			sendRequest(command, node);
			receiveResponse(command, node);
		} catch (IOException e) {
			logger.error("Failed to communicate with " + node, e);
			node.reconnect();
			throw e;
		} finally {
			lock.unlock();
		}
	}
	
	void sendRequest(Command command, TokyoTyrantNode node) throws IOException {
		ByteBuffer buffer = command.encode();
		node.write(buffer);
		logger.debug("Sent message " + buffer);
	}
	
	void receiveResponse(Command command, TokyoTyrantNode node) throws IOException {
		final int fragmentCapacity = 2048;
		ByteBuffer buffer = ByteBuffer.allocate(fragmentCapacity);
		ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
		
		int oldPos = 0;
		buffer.flip();
		while (!command.decode(buffer)) {
			logger.debug("Trying to read fragment");
			fragment.clear();
			int n = node.read(fragment);
			if (n == -1) {
				throw new IOException("Connection closed unexpectedly");
			}
			fragment.flip();
			logger.debug("Received fragment " + fragment);
			
			buffer.position(oldPos);
			buffer.limit(buffer.capacity());
			buffer = BufferHelper.accumulateBuffer(buffer, fragment);
			oldPos = buffer.position();
			buffer.flip();
		}
		logger.debug("Received message " + buffer);
	}
	
	public static class SynchronousServerNode implements TokyoTyrantNode {
		private final Logger logger = LoggerFactory.getLogger(getClass());
		private SocketAddress address;
		private int timeout;
		private volatile int reconnectAttempt = 1;
		
		private Socket socket;
		private InputStream inputStream;
		private OutputStream outputStream;

		public SynchronousServerNode(SocketAddress address, int timeout) {
			this.address = address;
			this.timeout = timeout;
		}

		public final boolean isActive() {
			return reconnectAttempt == 0 && socket != null && socket.isConnected();
		}

		public void connect() {
			try {
				socket = new Socket();
				socket.setSoTimeout(timeout);
				socket.connect(address, timeout);
				inputStream = socket.getInputStream();
				outputStream = socket.getOutputStream();
				reconnectAttempt = 0;
			} catch (IOException e) {
				logger.error("Cannot open connection to " + address, e);
			}
		}
		
		public void disconnect() {
			if (socket.isClosed()) return;
			try {
				socket.close();
			} catch (IOException e) {
				logger.error("Error while closing connection " + socket, e);
			}
		}
		
		public void reconnect() {
			logger.info("Reconnecting to " + address);
			reconnectAttempt++;
			disconnect();
			connect();
		}
		
		public void write(ByteBuffer buffer) throws IOException {
			//In blocking-mode, a write operation will return only after writing all of the requested bytes.
			outputStream.write(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
			buffer.position(buffer.limit());
		}
		
		public int read(ByteBuffer buffer) throws IOException {
			int n = inputStream.read(buffer.array(), buffer.position(), buffer.capacity());
			if (n == -1) {
				return n;
			}
			buffer.position(buffer.position() + n);
			return n;
		}
	}
}
