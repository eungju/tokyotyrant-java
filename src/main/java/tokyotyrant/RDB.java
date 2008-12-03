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
	/**
	 * scripting extension option: record locking
	 */
	public static final int XOLCKREC = 1 << 0;
	/**
	 * scripting extension option: global locking 
	 */
	public static final int XOLCKGLB = 1 << 1;
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private Transcoder keyTranscoder = new StringTranscoder();
	private Transcoder valueTranscoder = new StringTranscoder();
	private Socket socket;
	InputStream inputStream;
	OutputStream outputStream;

	/**
	 * Open a remote database connection.
	 * 
	 * @param address specifies the address of the server.
	 */
	public void open(SocketAddress address) throws IOException {
		open(address, 0);
	}

	/**
	 * Open a remote database connection.
	 * 
	 * @param address specifies the address of the server.
	 * @param timeout specified the socket timeout.
	 */
	public void open(SocketAddress address, int timeout) throws IOException {
		socket = new Socket();
		socket.setSoTimeout(timeout);
		socket.connect(address, timeout);
		inputStream = socket.getInputStream();
		outputStream = socket.getOutputStream();
	}

	/**
	 * Close the database connection.
	 */
	public void close() {
		if (socket == null) {
			return;
		}
		try {
			socket.close();
		} catch (IOException e) {
			logger.error("Error while closing connection " + socket, e);
		}
	}
	
	/**
	 * Execute the command.
	 * 
	 * @param <T> the type of the return value of the command.
	 * @param command the command to execute.
	 * @return the return value of the command.
	 */
	protected <T> T execute(Command<T> command) throws IOException {
		command.setKeyTranscoder(keyTranscoder);
		command.setValueTranscoder(valueTranscoder);
		sendRequest(command);
		receiveResponse(command);
		return command.getReturnValue();
	}

	/**
	 * Send request.
	 * 
	 * @param command the command to send request.
	 */
	void sendRequest(Command<?> command) throws IOException {
		ByteBuffer buffer = command.encode();
		//In blocking-mode, a write operation will return only after writing all of the requested bytes.
		outputStream.write(buffer.array(), 0, buffer.limit());
		logger.debug("Sent request " + buffer);
	}
	
	/**
	 * Receive response.
	 * 
	 * @param command the command to receive response.
	 */
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

	/**
	 * Store a record.
	 * If a record with the same key exists in the database, it is overwritten.
	 *  
	 * @param key specifies the key.
	 * @param value specifies the value.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean put(Object key, Object value) throws IOException {
		return execute(new Put(key, value));
	}

	/**
	 * Store a new record.
	 * If a record with the same key exists in the database, this method has no effect.
	 * 
	 * @param key specifies the key.
	 * @param value specifies the value.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean putkeep(Object key, Object value) throws IOException {
		return execute(new Putkeep(key, value));
	}

	/**
	 * Concatenate a value at the end of the existing record.
	 * If there is no corresponding record, a new record is created.
	 * 
	 * @param key specifies the key.
	 * @param value specifies the value.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean putcat(Object key, Object value) throws IOException {
		return execute(new Putcat(key, value));
	}

	/**
	 * Concatenate a value at the end of the existing record and rotate it to the left.
	 * If there is no corresponding record, a new record is created.
	 *  
	 * @param key specifies the key.
	 * @param value specifies the value.
	 * @param width specifies the width of the record.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean putrtt(Object key, Object value, int width) throws IOException {
		return execute(new Putrtt(key, value, width));
	}

	/**
	 * Store a record without response from the server
	 * If a record with the same key exists in the database, it is overwritten.
	 * 
	 * @param key specifies the key.
	 * @param value specifies the value.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public void putnr(Object key, Object value) throws IOException {
		execute(new Putnr(key, value));
	}

	/**
	 * Remove a record.
	 * 
	 * @param key specifies the key.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean out(Object key) throws IOException {
		return execute(new Out(key));
	}
	
	/**
	 * Retrieve a record.
	 * 
	 * @param key specifies the key.
	 * @return If successful, the return value is the value of the corresponding record. {@code null} is returned if no record corresponds.
	 */
	public Object get(Object key) throws IOException {
		return execute(new Get(key));
	}
	
	/**
	 * Retrieve records.
	 * 
	 * @param keys specifies an array containing the retrieval keys.
	 * @return If successful, the return value is the map contains corresponding values, else, it is {@code null}. As a result of this method, keys existing in the database have the corresponding values and keys not existing in the database are removed.
	 */
	public Map<Object, Object> mget(Object... keys) throws IOException {
		return execute(new Mget(keys));
	}

	/**
	 * Get the size of the value of a record.
	 * 
	 * @param key specifies the key.
	 * @return If successful, the return value is the size of the value of the corresponding record, else, it is -1.
	 */
	public int vsiz(Object key) throws IOException {
		return execute(new Vsiz(key));
	}

	/**
	 * Initialize the iterator.
	 * The iterator is used in order to access the key of every record stored in a database.
	 *  
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean iterinit() throws IOException {
		return execute(new Iterinit());
	}
	
	/**
	 * Get the next key of the iterator.
	 * It is possible to access every record by iteration of calling this method. It is allowed to update or remove records whose keys are fetched while the iteration. However, it is not assured if updating the database is occurred while the iteration. Besides, the order of this traversal access method is arbitrary, so it is not assured that the order of storing matches the one of the traversal access.
	 * 
	 * @return If successful, the return value is the next key, else, it is {@code null}. {@code null} is returned when no record is to be get out of the iterator.
	 */
	public Object iternext() throws IOException {
		return execute(new Iternext());
	}

	/**
	 * Get forward matching keys.
	 * Note that this method may be very slow because every key in the database is scanned.
	 * 
	 * @param prefix specifies the prefix of the corresponding keys.
	 * @param max specifies the maximum number of keys to be fetched. If it is not defined or negative, no limit is specified.
	 * @return The return value is an array of the keys of the corresponding records. This method does never fail and return an empty list even if no record corresponds.
	 */
	public List<Object> fwmkeys(Object prefix, int max) throws IOException {
		return execute(new Fwmkeys(prefix, max));
	}

	/**
	 * Add an integer to a record.
	 * If the corresponding record exists, the value is treated as an integer and is added to. If no record corresponds, a new record of the additional value is stored.
	 * 
	 * @param key specifies the key.
	 * @param num specifies the additional value. If it is not defined, 0 is specified.
	 * @return If successful, the return value is the summation value, else, it is {@link Integer#MIN_VALUE}.
	 */
	public int addint(Object key, int num) throws IOException {
		return execute(new Addint(key, num));
	}

	/**
	 * Add a real number to a record.
	 * If the corresponding record exists, the value is treated as a real number and is added to. If no record corresponds, a new record of the additional value is stored.
	 * 
	 * @param key specifies the key.
	 * @param num specifies the additional value. If it is not defined, 0 is specified.
	 * @return If successful, the return value is the summation value, else, it is {@link Double#NaN}.
	 */
	public double adddouble(Object key, double num) throws IOException {
		return execute(new Adddouble(key, num));
	}

	/**
	 * Call a function of the script language extension.
	 * 
	 * @param name specifies the function name.
	 * @param key specifies the key. If it is not defined, an empty string is specified.
	 * @param value specifies the value. If it is not defined, an empty string is specified.
	 * @param opts specifies options by bitwise or: {@link RDB#XOLCKREC} for record locking, {@link RDB#XOLCKGLB} for global locking. If it is {@code 0}, no option is specified.
	 * @return If successful, the return value is the value of the response or {@code null} on failure.
	 */
	public Object ext(String name, Object key, Object value, int opts) throws IOException {
		return execute(new Ext(name, key, value, opts));
	}

	/**
	 * Synchronize updated contents with the file and the device.
	 * This method is useful when another process connects the same database file.
	 * 
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean sync() throws IOException {
		return execute(new Sync());
	}

	/**
	 * Remove all records.
	 * 
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean vanish() throws IOException {
		return execute(new Vanish());
	}

	/**
	 * Copy the database file.
	 * The database file is assured to be kept synchronized and not modified while the copying or executing operation is in progress. So, this method is useful to create a backup file of the database file.
	 * 
	 * @param path specifies the path of the destination file. If it begins with {@code @}, the trailing substring is executed as a command line.
	 * @return If successful, the return value is true, else, it is false. False is returned if the executed command returns non-zero code.
	 */
	public boolean copy(String path) throws IOException {
		return execute(new Copy(path));
	}

	/**
	 * Restore the database file from the update log.
	 * 
	 * @param path specifies the path of the update log directory. If it begins with `+', the trailing substring is treated as the path and consistency checking is omitted.
	 * @param ts specifies the beginning time stamp in microseconds.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean restore(String path, long ts) throws IOException {
		return execute(new Restore(path, ts));
	}
	
	/**
	 * Set the replication master. 
	 * 
	 * @param host specifies the name or the address of the server. If it is {@code null}, replication of the database is disabled.
	 * @param port specifies the port number.
	 * @return If successful, the return value is true, else, it is false.
	 */
	public boolean setmst(String host, int port) throws IOException {
		return execute(new Setmst(host, port));
	}

	/**
	 * Get the number of records.
	 * 
	 * @return The return value is the number of records or 0 if the object does not connect to any database server.
	 */
	public long rnum() throws IOException {
		return execute(new Rnum());
	}

	/**
	 * Get the status string of the database server.
	 *  
	 * @return The return value is the status items of the database.
	 */
	public Map<String, String> stat() throws IOException {
		return execute(new Stat());
	}

	/**
	 * Get the size of the database.
	 *
	 * @return The return value is the size of the database or 0 if the object does not connect to any database server.
	 */
	public long size() throws IOException {
		return execute(new Size());
	}
	
	public static class Synchronized extends RDB {
		protected <T> T execute(Command<T> command) throws IOException {
			synchronized (this) {
				return super.execute(command);
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
			} else if ("iterinit".equals(command)) {
				System.out.println(db.iterinit());
			} else if ("iternext".equals(command)) {
				System.out.println(db.iternext());
			} else if ("list".equals(command)) {
				List<Object> keys = null;
				if (db.iterinit()) {
					keys = new ArrayList<Object>();
					while (true) {
						Object key = db.iternext();
						if (key == null) {
							break;
						}
						keys.add(key);
					}
				}
				System.out.println(keys);
			} else if ("fwmkeys".equals(command)) {
				System.out.println(db.fwmkeys(tokens[1], Integer.parseInt(tokens[2])));
			} else if ("addint".equals(command)) {
				System.out.println(db.addint(tokens[1], Integer.parseInt(tokens[2])));
			} else if ("adddouble".equals(command)) {
				System.out.println(db.adddouble(tokens[1], Double.parseDouble(tokens[2])));
			} else if ("ext".equals(command)) {
				System.out.println(db.ext(tokens[1], tokens[3], tokens[4], Integer.parseInt(tokens[2])));
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
