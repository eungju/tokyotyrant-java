package tokyotyrant.helper;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

public class BufferHelper {
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

    public static boolean prefixedDataAvailable(ChannelBuffer in, int prefixLength) {
    	return prefixedDataAvailable(in, prefixLength, Integer.MAX_VALUE);
    }

    public static boolean prefixedDataAvailable(ChannelBuffer in, int prefixLength, int maxDataLength) {
        if (in.readableBytes() < prefixLength) {
            return false;
        }

        long dataLength;
        switch (prefixLength) {
        case 1:
            dataLength = in.getUnsignedByte(in.readerIndex());
            break;
        case 2:
            dataLength = in.getUnsignedShort(in.readerIndex());
            break;
        case 4:
            dataLength = in.getUnsignedInt(in.readerIndex());
            break;
        default:
            throw new IllegalArgumentException("prefixLength: " + prefixLength);
        }

        if (dataLength < 0 || dataLength > maxDataLength) {
            throw new IllegalStateException("dataLength: " + dataLength);
        }

        return in.readableBytes() - prefixLength >= dataLength;
	}

	public static ByteBuffer expand(ByteBuffer buffer) {
		return expand(buffer, buffer.capacity());
	}

	public static ByteBuffer expand(ByteBuffer buffer, int capacity) {
		ByteBuffer expanded = ByteBuffer.allocate(buffer.capacity() + capacity);
		buffer.flip();
		expanded.put(buffer);
		return expanded;
	}

	private BufferHelper() {
	}
}
