package tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;

public class AsynchronousNetworking implements Networking, Runnable {
	private final Logger log = LoggerFactory.getLogger(getClass());  
	private final int fragmentCapacity = 2048;
	private Selector selector;
	private Thread ioThread;
	private boolean running;

	//to synchronize
	private CountDownLatch latch;
	private Command currentCommand;

	//per server
	private SocketAddress serverAddress;
	private SocketChannel channel;
	private ByteBuffer readBuffer;
	
	public AsynchronousNetworking(SocketAddress serverAddress) throws IOException {
		this.serverAddress = serverAddress;
		selector = Selector.open();
		ioThread = new Thread(this);
	}
	
	public void start() {
		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(serverAddress);
			channel.register(selector, SelectionKey.OP_CONNECT);
		} catch (IOException e) {
			log.error("Cannot open connection to " + serverAddress, e);
		}

		running = true;
		ioThread.start();
	}
	
	public void stop() {
		running = false;
		
		try {
			channel.close();
		} catch (IOException e) {
			log.error("Error while closing connection to " + serverAddress, e);
		}
		
		try {
			selector.close();
		} catch (IOException e) {
			log.error("Error while closing selector", e);
		}
	}
	
	public void run() {		
		while (running) {
			try {
				log.debug("Selecting...");
				selector.select();
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> i = selectedKeys.iterator();
				while (i.hasNext()) {
					SelectionKey key = i.next();
					if (key.isConnectable()) {
						log.debug("Ready to connect");
						((SocketChannel)key.channel()).finishConnect();
						key.channel().register(selector, SelectionKey.OP_WRITE);
						key.channel().register(selector, SelectionKey.OP_READ);
					}
					if (key.isWritable()) {
						log.debug("Ready to write");
					}
					if (key.isReadable()) {
						log.debug("Ready to read");

						ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
						if (channel.read(fragment) == -1) {
							log.info("Channel {} closed", channel);
						} else {
							fragment.flip();
							log.debug("Received fragment {}", fragment);
							received(fragment);
						}
					}
					i.remove();
				}
			} catch (Exception e) {
				log.error("Error while processing network communication", e);
			}
		}
	}

	void received(ByteBuffer fragment) throws IOException {
		if (readBuffer == null) {
			readBuffer = ByteBuffer.allocate(fragmentCapacity);
		}
		
		readBuffer = BufferHelper.accumulateBuffer(readBuffer, fragment);
		int pos = readBuffer.position();
		readBuffer.flip();
		if (currentCommand.decode(readBuffer)) {
			log.debug("Received message " + readBuffer);
			latch.countDown();
			
			if (readBuffer.hasRemaining()) {
				ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.remaining() + fragmentCapacity);
				newBuffer.put(readBuffer);
				readBuffer = newBuffer;
			} else {
				readBuffer = null;
			}
		} else {
			readBuffer.position(pos);
		}
	}
	
	public void execute(Command command) throws IOException {
		currentCommand = command;
		latch = new CountDownLatch(1);
		//channel.register(selector, SelectionKey.OP_WRITE);
		//channel.register(selector, SelectionKey.OP_READ);
		sendRequest(command, channel);
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.error("Interrupted", e);
		}
	}

	void sendRequest(Command command, ByteChannel channel) throws IOException {
		ByteBuffer buffer = command.encode();
		int written = 0;
		do {
			int n = channel.write(buffer);
			written += n;
		} while (written != buffer.limit());
		log.debug("Sent message " + buffer);
	}
}
