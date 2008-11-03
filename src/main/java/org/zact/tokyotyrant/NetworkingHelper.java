package org.zact.tokyotyrant;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkingHelper {
	private final Logger log = LoggerFactory.getLogger(getClass());  

	public ByteBuffer accumulateBuffer(ByteBuffer buffer, ByteBuffer addition) {
		log.debug("Buffer " + buffer);
		if (buffer.remaining() < addition.remaining()) {
			ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
			buffer.flip();
			newBuffer.put(buffer);
			buffer = newBuffer;
			log.debug("New buffer " + buffer);
		}
		buffer.put(addition);
		log.debug("Filled buffer " + buffer);
		return buffer;
	}
}
