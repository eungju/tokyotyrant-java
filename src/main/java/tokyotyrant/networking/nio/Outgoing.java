package tokyotyrant.networking.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import tokyotyrant.protocol.Command;

public class Outgoing {
	private final Incoming incoming;
	private final int bufferHighWatermark;
	private final ChannelBuffer buffer;
	private final BlockingQueue<Command<?>> writingCommands;
	private SocketChannel channel;

	public Outgoing(Incoming incoming, int bufferCapacity, int bufferHighWatermark) {
		this(incoming, bufferHighWatermark, ChannelBuffers.dynamicBuffer(bufferCapacity));
	}

	public Outgoing(Incoming incoming, int bufferHighWatermark, ChannelBuffer buffer) {
		this.incoming = incoming;
		this.bufferHighWatermark = bufferHighWatermark;
		this.buffer = buffer;
		writingCommands = new LinkedBlockingQueue<Command<?>>();
	}

	public void attach(SocketChannel channel) {
		this.channel = channel;
	}
	
	public void put(Command<?> command) {
		writingCommands.add(command);
	}
	
	public boolean hasWritable() {
		return buffer.readable() || !writingCommands.isEmpty();
	}
	
	public void write() throws Exception {
		fillBuffer();
		consumeBuffer();
	}

	void fillBuffer() throws Exception {
		while (!writingCommands.isEmpty() && buffer.readableBytes() < bufferHighWatermark) {
			Command<?> command = writingCommands.peek();
			try {
				command.encode(buffer);
				Command<?> _removed = writingCommands.remove();
				assert _removed == command;
				command.reading();
				incoming.put(command);
			} catch (Exception exception) {
				command.error(exception);
				throw new Exception("Error while sending " + command, exception);
			}
		}
	}
	
	void consumeBuffer() throws IOException {
		ByteBuffer chunk = buffer.toByteBuffer();
		int n = channel.write(chunk);
		buffer.skipBytes(n);
		buffer.discardReadBytes();
	}
	
	public void cancelAll() {
		buffer.clear();
	}
}
