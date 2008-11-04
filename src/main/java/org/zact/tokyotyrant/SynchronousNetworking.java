package org.zact.tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronousNetworking implements Networking {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private SocketAddress serverAddress;
	private SocketChannel channel;

	public SynchronousNetworking(SocketAddress serverAddress) {
		this.serverAddress = serverAddress;
	}

	public void start() {
    	channel = openConnection(serverAddress);
	}

	public void stop() {
		closeConnection(channel);
	}

	SocketChannel openConnection(SocketAddress address) {
		try {
			return SocketChannel.open(address);
		} catch (IOException e) {
			log.error("Cannot open connection to {}" + address, e);
		}
		return null;
	}

	void closeConnection(SocketChannel channel) {
		try {
			channel.close();
		} catch (IOException e) {
			log.error("Error while closing connection to " + serverAddress, e);
		}
	}

	public void execute(Command command) throws IOException {
		sendRequest(command, channel);
		receiveResponse(command, channel);
	}
	
	void sendRequest(Command command, ByteChannel channel) throws IOException {
		//In blocking-mode, a write operation will return only after writing all of the requested bytes.
		ByteBuffer buffer = command.encode();
		if (channel.write(buffer) == -1) {
			log.info("EOF");
		}
		log.debug("Sent message " + buffer);
	}
	
	void receiveResponse(Command command, ByteChannel channel) throws IOException {
		final int fragmentCapacity = 2048;
		ByteBuffer buffer = ByteBuffer.allocate(fragmentCapacity);
		ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
		
		buffer.flip();
		int oldPos = 0;
		while (!command.decode(buffer)) {
			log.debug("Trying to read fragment");
			fragment.clear();
			if (channel.read(fragment) == -1) {
				log.info("EOF");
				break;
			}
			fragment.flip();
			log.debug("Received fragment " + fragment);
			
			buffer.position(oldPos);
			buffer.limit(buffer.capacity());
			buffer = BufferHelper.accumulateBuffer(buffer, fragment);
			oldPos = buffer.position();
			buffer.flip();
		}
		log.debug("Received message " + buffer + ", " + command.code);
	}
}
