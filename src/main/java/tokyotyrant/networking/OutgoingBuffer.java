package tokyotyrant.networking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;
import tokyotyrant.protocol.Command;

public class OutgoingBuffer {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private int initialCapacity;
	private ByteBuffer buffer;
	private int remaining;
	private boolean filling;
	
	public OutgoingBuffer(int initialCapacity) {
		this.initialCapacity = initialCapacity;
		buffer = ByteBuffer.allocate(initialCapacity);
		clear();
	}
	
	/**
	 * Clear the buffer and prepare to fill.
	 */
	public void clear() {
		filling = true;
		buffer.clear();
		remaining = 0;
	}

	/**
	 * Start or continue to fill the buffer.
	 */
	public void fill(Command<?> command) {
		if (!filling) {
			filling = true;
			if (buffer.hasRemaining()) {
				throw new IllegalStateException("Cannot start to fill");
			} else {
				buffer.clear();
			}
		}
		
		ByteBuffer chunk = command.encode();
		int chunkSize = chunk.remaining();
		if (chunkSize > buffer.remaining()) {
			buffer = BufferHelper.expand(buffer, Math.max(chunkSize, buffer.capacity()));
		}
		buffer.put(chunk);
		remaining += chunkSize;
		logger.debug("Filled {} bytes", chunkSize);
	}

	/**
	 * Start or continue to consume the buffer. 
	 */
	public void consume(WritableByteChannel channel) throws IOException {
		if (filling) {
			filling = false;
			buffer.flip();
			logger.debug("Will consume {} bytes", remaining);
		}
		
		int n = channel.write(buffer);
		remaining -= n;
		logger.debug("Sent {} bytes", n);
	}

	public boolean isEmpty() {
		return remaining == 0;
	}
	
	public boolean isHungry() {
		return remaining < initialCapacity;
	}
}
