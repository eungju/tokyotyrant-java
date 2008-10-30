package org.zact.tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TyrantClient {
	private final Logger log = LoggerFactory.getLogger(getClass());  
	private int timeout = 1000;
	private SocketChannel channel;
    private Transcoder defaultTranscoder = new StringTranscoder();
    
    public TyrantClient(String host, int port) throws IOException {
        connect(new InetSocketAddress(host, port));
	}

    public void connect(InetSocketAddress remoteAddress) throws IOException {
    	try {
			channel = SocketChannel.open(remoteAddress);
			channel.socket().setSoTimeout(timeout);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
	public void close() {
		try {
			channel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void dispose() {
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
	
	void sendAndReceive(Command command, ByteChannel channel) throws IOException {
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
	}
	
	protected Transcoder getTranscoder() {
		return defaultTranscoder;
	}
	
	public boolean put(Object key, Object value) throws IOException {
		Put command = new Put(key, value);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public boolean putkeep(Object key, Object value) throws IOException {
		Putkeep command = new Putkeep(key, value);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public boolean putcat(Object key, Object value) throws IOException {
		Putcat command = new Putcat(key, value);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public boolean putrtt(Object key, Object value, int width) throws IOException {
		Putrtt command = new Putrtt(key, value, width);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public void putnr(Object key, Object value) throws IOException {
		Putnr command = new Putnr(key, value);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
	}

	public boolean out(Object key) throws IOException {
		Out command = new Out(key);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}
	
	public Object get(Object key) throws IOException {
		Get command = new Get(key);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}
	
	public Map<Object, Object> mget(Object[] keys) throws IOException {
		Mget command = new Mget(keys);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public int vsiz(Object key) throws IOException {
		Vsiz command = new Vsiz(key);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public boolean setmst(String host, int port) throws IOException {
		Setmst command = new Setmst(host, port);
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public long rnum() throws IOException {
		Rnum command = new Rnum();
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public String stat() throws IOException {
		Stat command = new Stat();
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}

	public long size() throws IOException {
		Size command = new Size();
		command.setTranscoder(getTranscoder());
		sendAndReceive(command, channel);
		return command.getReturnValue();
	}
}
