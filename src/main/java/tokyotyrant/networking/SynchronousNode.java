package tokyotyrant.networking;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;
import tokyotyrant.protocol.Command;

public class SynchronousNode implements TokyoTyrantNode, Runnable {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private SocketAddress address;
	private int timeout;
	private volatile int reconnectAttempt = 1;
	
	private BlockingQueue<Command<?>> ioQueue;
	private Thread ioThread;
	private boolean running;
	
	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;

	public SynchronousNode(SocketAddress address, int timeout) {
		this.address = address;
		this.timeout = timeout;
		this.ioQueue = new ArrayBlockingQueue<Command<?>>(16 * 1024);
		
		ioThread = new Thread(this);
	}

	public void start() {
		connect();
		running = true;
		ioThread.start();
	}
	
	public void stop() {
		disconnect();
		running = false;
	}

	public void run() {
		while (running) {
			Command<?> command = null;
			try {
				command = ioQueue.poll(1, TimeUnit.SECONDS);
			} catch (InterruptedException ignore) {
			}
			if (command == null || command.isCancelled()) {
				continue;
			}
			
			try {
				sendRequest(command);
				command.reading();
				receiveResponse(command);
				command.complete();
			} catch (IOException e) {
				command.error(e);
				logger.error("Failed to communicate with " + address, e);
				reconnect();
			}
		}
	}

	public void send(Command<?> command) {
		ioQueue.add(command);
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
			connected();
		} catch (IOException e) {
			logger.error("Cannot open connection to " + address, e);
		}
	}

	public void connected() {
		reconnectAttempt = 0;
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

	void sendRequest(Command<?> command) throws IOException {
		ByteBuffer buffer = command.encode();
		write(buffer);
		logger.debug("Sent message " + buffer);
	}
	
	void receiveResponse(Command<?> command) throws IOException {
		final int fragmentCapacity = 2048;
		ByteBuffer buffer = ByteBuffer.allocate(fragmentCapacity);
		ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
		
		int oldPos = 0;
		buffer.flip();
		while (!command.decode(buffer)) {
			logger.debug("Trying to read fragment");
			fragment.clear();
			int n = read(fragment);
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

	void write(ByteBuffer buffer) throws IOException {
		//In blocking-mode, a write operation will return only after writing all of the requested bytes.
		outputStream.write(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
		buffer.position(buffer.limit());
	}
	
	int read(ByteBuffer buffer) throws IOException {
		int n = inputStream.read(buffer.array(), buffer.position(), buffer.capacity());
		if (n == -1) {
			return n;
		}
		buffer.position(buffer.position() + n);
		return n;
	}
}