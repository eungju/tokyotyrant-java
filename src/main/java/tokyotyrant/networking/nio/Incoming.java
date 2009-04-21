package tokyotyrant.networking.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.protocol.Command;

public class Incoming {
	private final Logger logger = LoggerFactory.getLogger(getClass());  
	private final int bufferCapacity;
	private final ChannelBuffer buffer;
	private final BlockingQueue<Command<?>> readingCommands;
	private SocketChannel channel;
	
	public Incoming(int bufferCapacity) {
		this(bufferCapacity, ChannelBuffers.dynamicBuffer(bufferCapacity));
	}

	public Incoming(int bufferCapacity, ChannelBuffer buffer) {
		this.bufferCapacity = bufferCapacity;
		this.buffer = buffer;
		readingCommands = new LinkedBlockingQueue<Command<?>>();
	}

	public void attach(SocketChannel channel) {
		this.channel = channel;
	}
	
	public void put(Command<?> command) {
		command.reading();
		readingCommands.add(command);
	}
	
	public boolean hasReadable() {
		return !readingCommands.isEmpty();
	}
	
	public void read() throws Exception {
		fillBuffer();
		consumeBuffer();
	}

	void fillBuffer() throws IOException {
		ByteBuffer chunk = ByteBuffer.allocate(bufferCapacity);
		int n = channel.read(chunk);
		if (n == -1) {
			throw new IOException("Channel " + channel + " is closed");
		}
		chunk.flip();
		buffer.writeBytes(chunk);
	}
	
	void consumeBuffer() throws Exception {
		while (!readingCommands.isEmpty()) {
			Command<?> command = readingCommands.peek();
			try {
				buffer.markReaderIndex();
				if (!command.decode(buffer)) {
					buffer.resetReaderIndex();
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
		buffer.discardReadBytes();
	}
	
	public void cancelAll() {
		buffer.clear();
		int count = 0;
		for (Iterator<Command<?>> i = readingCommands.iterator(); i.hasNext(); ) {
			Command<?> each = i.next();
			each.cancel();
			i.remove();
			count++;
		}
		logger.warn("{} commands are cancelled", count);
		assert readingCommands.isEmpty();
	}
}
