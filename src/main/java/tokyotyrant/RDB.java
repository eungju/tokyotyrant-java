package tokyotyrant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.command.Adddouble;
import tokyotyrant.command.Addint;
import tokyotyrant.command.Copy;
import tokyotyrant.command.Ext;
import tokyotyrant.command.Fwmkeys;
import tokyotyrant.command.Get;
import tokyotyrant.command.Iterinit;
import tokyotyrant.command.Iternext;
import tokyotyrant.command.Mget;
import tokyotyrant.command.Out;
import tokyotyrant.command.Put;
import tokyotyrant.command.Putcat;
import tokyotyrant.command.Putkeep;
import tokyotyrant.command.Putnr;
import tokyotyrant.command.Putrtt;
import tokyotyrant.command.Restore;
import tokyotyrant.command.Rnum;
import tokyotyrant.command.Setmst;
import tokyotyrant.command.Size;
import tokyotyrant.command.Stat;
import tokyotyrant.command.Sync;
import tokyotyrant.command.Vanish;
import tokyotyrant.command.Vsiz;
import tokyotyrant.helper.BufferHelper;

/**
 * Tokyo Tyrant C/Perl/Ruby API like interface.
 */
public class RDB {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private Transcoder transcoder = new StringTranscoder();
	private int timeout = 1000;
	private Socket socket;
	InputStream inputStream;
	OutputStream outputStream;

	public void open(SocketAddress address) throws IOException {
		socket = new Socket();
		socket.setSoTimeout(timeout);
		socket.connect(address, timeout);
		inputStream = socket.getInputStream();
		outputStream = socket.getOutputStream();
	}
	
	public void close() {
		if (socket == null) {
			return;
		}
		try {
			socket.close();
		} catch (IOException ignore) {
		}
	}
	
	protected <T> T execute(Command<T> command) throws IOException {
		command.setTranscoder(transcoder);
		sendRequest(command);
		receiveResponse(command);
		return command.getReturnValue();
	}

	void sendRequest(Command<?> command) throws IOException {
		ByteBuffer buffer = command.encode();
		//In blocking-mode, a write operation will return only after writing all of the requested bytes.
		outputStream.write(buffer.array(), 0, buffer.limit());
		logger.debug("Sent request " + buffer);
	}
	
	void receiveResponse(Command<?> command) throws IOException {
		final int fragmentCapacity = 2048;
		ByteBuffer buffer = ByteBuffer.allocate(fragmentCapacity);
		ByteBuffer fragment = ByteBuffer.allocate(fragmentCapacity);
		
		int oldPos = 0;
		buffer.flip();
		while (!command.decode(buffer)) {
			logger.debug("Trying to read fragment");
			fragment.clear();
			int n = inputStream.read(fragment.array(), 0, fragment.capacity());
			if (n == -1) {
				throw new IOException("Connection closed unexpectedly");
			}
			fragment.position(0);
			fragment.limit(n);
			logger.debug("Received fragment " + fragment);
			
			buffer.position(oldPos);
			buffer.limit(buffer.capacity());
			buffer = BufferHelper.accumulateBuffer(buffer, fragment);
			oldPos = buffer.position();
			buffer.flip();
		}
		logger.debug("Received response " + buffer);
	}

	public boolean put(Object key, Object value) throws IOException {
		return execute(new Put(key, value));
	}

	public boolean putkeep(Object key, Object value) throws IOException {
		return execute(new Putkeep(key, value));
	}

	public boolean putcat(Object key, Object value) throws IOException {
		return execute(new Putcat(key, value));
	}

	public boolean putrtt(Object key, Object value, int width) throws IOException {
		return execute(new Putrtt(key, value, width));
	}

	public void putnr(Object key, Object value) throws IOException {
		execute(new Putnr(key, value));
	}

	public boolean out(Object key) throws IOException {
		return execute(new Out(key));
	}
	
	public Object get(Object key) throws IOException {
		return execute(new Get(key));
	}
	
	public Map<Object, Object> mget(Object... keys) throws IOException {
		return execute(new Mget(keys));
	}

	public int vsiz(Object key) throws IOException {
		return execute(new Vsiz(key));
	}

	public boolean iterinit() throws IOException {
		return execute(new Iterinit());
	}
	
	public Object iternext() throws IOException {
		return execute(new Iternext());
	}

	public List<Object> list() throws IOException {
		if (!iterinit()) {
			return null;
		}

		List<Object> result = new ArrayList<Object>();
		while (true) {
			Object key = iternext();
			if (key == null) {
				break;
			}
			result.add(key);
		}
		return result;
	}

	public List<Object> fwmkeys(Object prefix, int max) throws IOException {
		return execute(new Fwmkeys(prefix, max));
	}

	public int addint(Object key, int num) throws IOException {
		return execute(new Addint(key, num));
	}

	public double adddouble(Object key, double num) throws IOException {
		return execute(new Adddouble(key, num));
	}

	public Object ext(String name, int opts, Object key, Object value) throws IOException {
		return execute(new Ext(name, opts, key, value));
	}

	public boolean sync() throws IOException {
		return execute(new Sync());
	}

	public boolean vanish() throws IOException {
		return execute(new Vanish());
	}

	public boolean copy(String path) throws IOException {
		return execute(new Copy(path));
	}

	public boolean restore(String path, long ts) throws IOException {
		return execute(new Restore(path, ts));
	}
	
	public boolean setmst(String host, int port) throws IOException {
		return execute(new Setmst(host, port));
	}

	public long rnum() throws IOException {
		return execute(new Rnum());
	}

	public Map<String, String> stat() throws IOException {
		return execute(new Stat());
	}

	public long size() throws IOException {
		return execute(new Size());
	}
	
	public static class Synchronized extends RDB {
		protected <T> T execute(Command<T> command) throws IOException {
			synchronized (this) {
				return super.execute(command);
			}
		}
		
		public List<Object> list() throws IOException {
			synchronized (this) {
				return super.list();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		RDB db = new RDB();
		db.open(new InetSocketAddress(args[0], Integer.parseInt(args[1])));
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.print("> ");
			String[] tokens = reader.readLine().split("\\s");
			String command = tokens[0];
			long s = System.currentTimeMillis();
			if ("put".equals(command)) {
				System.out.println(db.put(tokens[1], tokens[2]));
			} else if ("putkeep".equals(command)) {
				System.out.println(db.putkeep(tokens[1], tokens[2]));
			} else if ("putcat".equals(command)) {
				System.out.println(db.putcat(tokens[1], tokens[2]));
			} else if ("putrtt".equals(command)) {
				System.out.println(db.putrtt(tokens[1], tokens[2], Integer.parseInt(tokens[3])));
			} else if ("putnr".equals(command)) {
				db.putnr(tokens[1], tokens[2]);
			} else if ("out".equals(command)) {
				System.out.println(db.out(tokens[1]));
			} else if ("get".equals(command)) {
				System.out.println(tokens[1] + "\t" + db.get(tokens[1]));
			} else if ("mget".equals(command)) {
				Object[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
				Map<Object, Object> values = db.mget(keys);
				for (Object key : values.keySet()) {
					System.out.println(key + "\t" + values.get(key));
				}
			} else if ("vsiz".equals(command)) {
				System.out.println(db.vsiz(tokens[1]));
			} else if ("list".equals(command)) {
				System.out.println(db.list());
			} else if ("fwmkeys".equals(command)) {
				System.out.println(db.fwmkeys(tokens[1], Integer.parseInt(tokens[2])));
			} else if ("addint".equals(command)) {
				System.out.println(db.addint(tokens[1], Integer.parseInt(tokens[2])));
			} else if ("adddouble".equals(command)) {
				System.out.println(db.adddouble(tokens[1], Double.parseDouble(tokens[2])));
			} else if ("ext".equals(command)) {
				System.out.println(db.ext(tokens[1], Integer.parseInt(tokens[2]), tokens[3], tokens[4]));
			} else if ("sync".equals(command)) {
				System.out.println(db.sync());
			} else if ("vanish".equals(command)) {
				System.out.println(db.vanish());
			} else if ("copy".equals(command)) {
				System.out.println(db.copy(tokens[1]));
			} else if ("restore".equals(command)) {
				System.out.println(db.restore(tokens[1], Long.parseLong(tokens[2])));
			} else if ("setmst".equals(command)) {
				System.out.println(db.setmst(tokens[1], Integer.parseInt(tokens[2])));
			} else if ("rnum".equals(command)) {
				System.out.println(db.rnum());
			} else if ("stat".equals(command)) {
				System.out.println(db.stat());
			} else if ("size".equals(command)) {
				System.out.println(db.size());
			} else if ("quit".equals(command)) {
				db.close();
				break;
			}
			System.out.println("It takes " + (System.currentTimeMillis() - s) + "ms");
		}
	}
}
