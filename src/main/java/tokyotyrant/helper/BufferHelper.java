package tokyotyrant.helper;

import java.nio.ByteBuffer;

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

	public static ByteBuffer expand(ByteBuffer buffer) {
		ByteBuffer expanded = ByteBuffer.allocate(buffer.capacity() * 2);
		buffer.flip();
		expanded.put(buffer);
		return expanded;
	}
	
	private BufferHelper() {
	}
}
