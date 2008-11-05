package tokyotyrant.helper;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferHelper {
	private static final Logger log = LoggerFactory.getLogger(BufferHelper.class);  
	
    public static boolean prefixedDataAvailable(ByteBuffer in, int prefixLength) {
    	return prefixedDataAvailable(in, prefixLength, Integer.MAX_VALUE);
    }

    public static boolean prefixedDataAvailable(ByteBuffer in, int prefixLength, int maxDataLength) {
        if (in.remaining() < prefixLength) {
            return false;
        }

        int dataLength;
        switch (prefixLength) {
        case 1:
            dataLength = in.get(in.position()) & 0xff;
            break;
        case 2:
            dataLength = in.getShort(in.position()) & 0xffff;
            break;
        case 4:
            dataLength = in.getInt(in.position()) & 0xffffffff;
            break;
        default:
            throw new IllegalArgumentException("prefixLength: " + prefixLength);
        }

        if (dataLength < 0 || dataLength > maxDataLength) {
            throw new IllegalStateException("dataLength: " + dataLength);
        }

        return in.remaining() - prefixLength >= dataLength;
	}

	public static ByteBuffer accumulateBuffer(ByteBuffer buffer, ByteBuffer addition) {
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
	
	private BufferHelper() {
	}
}
