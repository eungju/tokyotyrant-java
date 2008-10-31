package org.zact.tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
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
	
	void execute(Command command) throws IOException {
		command.setTranscoder(getTranscoder());
		cumulativeWrite(command, channel);
		cumulativeRead(command, channel);
	}
	
	protected Transcoder getTranscoder() {
		return defaultTranscoder;
	}
	
	public boolean put(Object key, Object value) throws IOException {
		Put command = new Put(key, value);
		execute(command);
		return command.getReturnValue();
	}

	public boolean putkeep(Object key, Object value) throws IOException {
		Putkeep command = new Putkeep(key, value);
		execute(command);
		return command.getReturnValue();
	}

	public boolean putcat(Object key, Object value) throws IOException {
		Putcat command = new Putcat(key, value);
		execute(command);
		return command.getReturnValue();
	}

	public boolean putrtt(Object key, Object value, int width) throws IOException {
		Putrtt command = new Putrtt(key, value, width);
		execute(command);
		return command.getReturnValue();
	}

	public void putnr(Object key, Object value) throws IOException {
		Putnr command = new Putnr(key, value);
		execute(command);
	}

	public boolean out(Object key) throws IOException {
		Out command = new Out(key);
		execute(command);
		return command.getReturnValue();
	}
	
	public Object get(Object key) throws IOException {
		Get command = new Get(key);
		execute(command);
		return command.getReturnValue();
	}
	
	public Map<Object, Object> mget(Object[] keys) throws IOException {
		Mget command = new Mget(keys);
		execute(command);
		return command.getReturnValue();
	}

	public int vsiz(Object key) throws IOException {
		Vsiz command = new Vsiz(key);
		execute(command);
		return command.getReturnValue();
	}

	public boolean iterinit() throws IOException {
		Iterinit command = new Iterinit();
		execute(command);
		return command.getReturnValue();
	}
	
	public Object iternext() throws IOException {
		Iternext command = new Iternext();
		execute(command);
		return command.getReturnValue();
	}

	public List<Object> list() throws IOException {
		List<Object> result = null;
		if (iterinit()) {
			result = new ArrayList<Object>();
			while (true) {
				Object key = iternext();
				if (key == null) {
					break;
				}
				result.add(key);
			}
		}
		return result;
	}

	public List<Object> fwmkeys(Object prefix, int max) throws IOException {
		Fwmkeys command = new Fwmkeys(prefix, max);
		execute(command);
		return command.getReturnValue();
	}

	public int addint(Object key, int num) throws IOException {
		Addint command = new Addint(key, num);
		execute(command);
		return command.getReturnValue();
	}

	public double adddouble(Object key, double num) throws IOException {
		Adddouble command = new Adddouble(key, num);
		execute(command);
		return command.getReturnValue();
	}

	public Object ext(String name, int opts, Object key, Object value) throws IOException {
		Ext command = new Ext(name, opts, key, value);
		execute(command);
		return command.getReturnValue();
	}

	public boolean sync() throws IOException {
		Sync command = new Sync();
		execute(command);
		return command.getReturnValue();
	}

	public boolean vanish() throws IOException {
		Vanish command = new Vanish();
		execute(command);
		return command.getReturnValue();
	}

	public boolean copy(String path) throws IOException {
		Copy command = new Copy(path);
		execute(command);
		return command.getReturnValue();
	}

	public boolean restore(String path, long ts) throws IOException {
		Restore command = new Restore(path, ts);
		execute(command);
		return command.getReturnValue();
	}
	
	public boolean setmst(String host, int port) throws IOException {
		Setmst command = new Setmst(host, port);
		execute(command);
		return command.getReturnValue();
	}

	public long rnum() throws IOException {
		Rnum command = new Rnum();
		execute(command);
		return command.getReturnValue();
	}

	public Map<String, String> stat() throws IOException {
		Stat command = new Stat();
		execute(command);
		return command.getReturnValue();
	}

	public long size() throws IOException {
		Size command = new Size();
		execute(command);
		return command.getReturnValue();
	}
}
