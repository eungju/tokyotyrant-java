package org.zact.tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
	
	void cumulativeWrite(Command command, SocketChannel channel) throws IOException {
		ByteBuffer buffer = command.encode();
		int n = 0;
		do {
			n += channel.write(buffer);
		} while (n < buffer.limit());
		log.info("Sent message " + buffer);
	}
	
	void cumulativeRead(Command command, SocketChannel channel) throws IOException {
		ByteBuffer buffer = null;
		ByteBuffer received = ByteBuffer.allocate(1024);
		do {
			buffer = ByteBuffer.allocate(received.limit() * 2);
			received.flip();
			buffer.put(received);
			channel.read(buffer);
			received = buffer;
			buffer.flip();
		} while (!command.decode(buffer));
		log.info("Received message " + buffer);
	}
	
	protected Transcoder getTranscoder() {
		return defaultTranscoder;
	}
	
	public boolean put(Object key, Object value) throws IOException {
		Put command = new Put(key, value);
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
		return command.isSuccess();
	}

	public boolean out(Object key) throws IOException {
		Out command = new Out(key);
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
		return command.isSuccess();
	}
	
	public Object get(Object key) throws IOException {
		Get command = new Get(key);
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
		return command.getValue();
	}
	
	public Map<Object, Object> mget(Object[] keys) throws IOException {
		Mget command = new Mget(keys);
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
		return command.getValue();
	}

	public int vsiz(Object key) throws IOException {
		Vsiz command = new Vsiz(key);
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
		return command.getValue();
	}
}
