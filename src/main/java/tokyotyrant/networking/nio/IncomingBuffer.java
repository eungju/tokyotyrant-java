package tokyotyrant.networking.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.helper.BufferHelper;
import tokyotyrant.protocol.Command;

public class IncomingBuffer {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private int initialCapacity;
	private ByteBuffer buffer;
	private boolean filling;
	
	public IncomingBuffer(int initialCapacity) {
		this.initialCapacity = initialCapacity;
		buffer = ByteBuffer.allocate(initialCapacity);
		clear();
	}
	
	/**
	 * Clear the buffer and prepare to fill.
	 */
	public void clear() {
		buffer.clear();
		filling = true;
	}

	public void fill(ReadableByteChannel channel) throws IOException {
		if (!filling) {
			filling = true;
			//TODO: need to shrink aggressively?
			if (buffer.hasRemaining()) {
				ByteBuffer newReadingBuffer = ByteBuffer.allocate(buffer.capacity());
				newReadingBuffer.put(buffer);
				buffer = newReadingBuffer;
			} else {
				buffer.clear();
			}
		}
		
		if (buffer.remaining() < initialCapacity) {
			buffer = BufferHelper.expand(buffer);
		}
		int n = channel.read(buffer);
		if (n == -1) {
			throw new IOException("Channel " + channel + " is closed");
		}
		logger.debug("Received {} bytes", n);
	}
	
	public boolean consume(Command<?> command) {
		if (filling) {
			filling = false;
			buffer.flip();
		}

		buffer.mark();
		logger.debug("Try to decode {}", buffer);
		if (command.decode(buffer)) {
			return true;
		} else {
			buffer.reset();
			return false;
		}
	}
}
