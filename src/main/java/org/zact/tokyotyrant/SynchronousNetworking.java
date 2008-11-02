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
    	try {
			channel = SocketChannel.open(serverAddress);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		try {
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public void execute(Command command) throws IOException {
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
	}
	
	void cumulativeWrite(Command command, ByteChannel channel) throws IOException {
		//In blocking-mode, a write operation will return only after writing all of the requested bytes.
		ByteBuffer buffer = command.encode();
		channel.write(buffer);
		log.debug("Sent message " + buffer);
	}
	
	ByteBuffer fillBuffer(ByteBuffer buffer, ByteBuffer more) {
		log.debug("buffer " + buffer);
		if (buffer.remaining() < more.remaining()) {
			ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
			buffer.flip();
			newBuffer.put(buffer);
			buffer = newBuffer;
		}
		log.debug("new buffer " + buffer);
		buffer.put(more);
		log.debug("filled buffer " + buffer);
		return buffer;
	}
	
	void cumulativeRead(Command command, ByteChannel channel) throws IOException {
		final int fragmentCapacity = 2048;
		ByteBuffer buffer = ByteBuffer.allocate(fragmentCapacity);
		ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
		
		buffer.flip();
		int oldPos = 0;
		while (!command.decode(buffer)) {
			log.debug("Trying to read fragment");
			fragment.clear();
			channel.read(fragment);
			fragment.flip();
			log.debug("Received fragment " + fragment);
			
			buffer.position(oldPos);
			buffer.limit(buffer.capacity());
			buffer = fillBuffer(buffer, fragment);
			oldPos = buffer.position();
			buffer.flip();
		}
		log.debug("Received message " + buffer + ", " + command.code);
	}
}
