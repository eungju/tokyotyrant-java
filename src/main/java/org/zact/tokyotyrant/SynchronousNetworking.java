package org.zact.tokyotyrant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronousNetworking implements Networking {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	/** In millisecond */
	private int timeout = 1000;
	private ServerNode serverNode;

	public SynchronousNetworking(SocketAddress serverAddress) {
		serverNode = new SynchronousServerNode(serverAddress, timeout);
	}

	public void start() {
		serverNode.connect();
	}

	public void stop() {
		serverNode.close();
	}

	public void execute(Command command) throws IOException {
		while (true) {
			try {
				sendRequest(command, serverNode);
				receiveResponse(command, serverNode);
				break;
			} catch (Exception e) {
				logger.error("Failed to communicate with " + serverNode, e);
				serverNode.reconnect();
			}
		}
	}
	
	void sendRequest(Command command, ServerNode node) throws IOException {
		ByteBuffer buffer = command.encode();
		node.write(buffer);
		logger.debug("Sent message " + buffer);
	}
	
	void receiveResponse(Command command, ServerNode node) throws IOException {
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
		logger.debug("Received message " + buffer + ", " + command.code);
	}
	
	public static class SynchronousServerNode implements ServerNode {
		private final Logger logger = LoggerFactory.getLogger(getClass());
		private SocketAddress address;
		private int timeout;
		private Socket socket;
		private InputStream inputStream;
		private OutputStream outputStream;

		public SynchronousServerNode(SocketAddress address, int timeout) {
			this.address = address;
			this.timeout = timeout;
		}
		
		public void connect() {
			try {
				socket = new Socket();
				socket.setSoTimeout(timeout);
				socket.connect(address, timeout);
				inputStream = socket.getInputStream();
				outputStream = socket.getOutputStream();
			} catch (IOException e) {
				logger.error("Cannot open connection to " + address, e);
			}
		}
		
		public void close() {
			try {
				socket.close();
			} catch (IOException e) {
				logger.error("Error while closing connection " + socket, e);
			}
		}
		
		public void reconnect() {
			logger.info("Reconnecting to " + address);
			connect();
			close();
		}
		
		public void write(ByteBuffer buffer) throws IOException {
			//In blocking-mode, a write operation will return only after writing all of the requested bytes.
			outputStream.write(buffer.array(), 0, buffer.limit());
			buffer.position(buffer.limit());
		}
		
		public int read(ByteBuffer buffer) throws IOException {
			int n = inputStream.read(buffer.array(), buffer.position(), buffer.capacity());
			if (n == -1) {
				return n;
			}
			buffer.position(n);
			return n;
		}
	}
}
