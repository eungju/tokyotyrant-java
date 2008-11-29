package tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

public class TokyoTyrantClient {
	private final Logger log = LoggerFactory.getLogger(getClass());  
    private Transcoder defaultTranscoder = new StringTranscoder();
	private Networking networking;
	private long globalTimeout = 1000L;
    
    public TokyoTyrantClient(String host, int port) throws IOException {
		log.info("Initializing...");
    	networking = new AsynchronousNetworking(new InetSocketAddress(host, port));
    	networking.start();
		log.info("Initialized");
	}
	
	public void dispose() {
		log.info("Disposing...");
		networking.stop();
		log.info("Disposed");
	}
	
	public void setGlobalTimeout(long timeout) {
		this.globalTimeout = timeout;
	}
	
	Transcoder getTranscoder() {
		return defaultTranscoder;
	}

	<T> Future<T> execute(Command<T> command) throws IOException {
		command.setTranscoder(getTranscoder());
		CommandFuture<T> future = new CommandFuture<T>(command, globalTimeout);
		networking.send(command);
		return future;
	}

	public Future<Boolean> put(Object key, Object value) throws IOException {
		Put command = new Put(key, value);
		return execute(command);
	}

	public Future<Boolean> putkeep(Object key, Object value) throws IOException {
		Putkeep command = new Putkeep(key, value);
		return execute(command);
	}

	public Future<Boolean> putcat(Object key, Object value) throws IOException {
		Putcat command = new Putcat(key, value);
		return execute(command);
	}

	public Future<Boolean> putrtt(Object key, Object value, int width) throws IOException {
		Putrtt command = new Putrtt(key, value, width);
		return execute(command);
	}

	public void putnr(Object key, Object value) throws IOException {
		Putnr command = new Putnr(key, value);
		execute(command);
	}

	public Future<Boolean> out(Object key) throws IOException {
		Out command = new Out(key);
		return execute(command);
	}
	
	public Future<Object> get(Object key) throws IOException {
		Get command = new Get(key);
		return execute(command);
	}
	
	public Future<Map<Object, Object>> mget(Object[] keys) throws IOException {
		Mget command = new Mget(keys);
		return execute(command);
	}

	public Future<Integer> vsiz(Object key) throws IOException {
		Vsiz command = new Vsiz(key);
		return execute(command);
	}

	public Future<Boolean> iterinit() throws IOException {
		Iterinit command = new Iterinit();
		return execute(command);
	}
	
	public Future<Object> iternext() throws IOException {
		Iternext command = new Iternext();
		return execute(command);
	}

	public List<Object> list() throws IOException, InterruptedException, ExecutionException {
		if (!iterinit().get()) {
			return null;
		}

		List<Object> result = new ArrayList<Object>();
		while (true) {
			Object key = iternext().get();
			if (key == null) {
				break;
			}
			result.add(key);
		}
		return result;
	}

	public Future<List<Object>> fwmkeys(Object prefix, int max) throws IOException {
		Fwmkeys command = new Fwmkeys(prefix, max);
		return execute(command);
	}

	public Future<Integer> addint(Object key, int num) throws IOException {
		Addint command = new Addint(key, num);
		return execute(command);
	}

	public Future<Double> adddouble(Object key, double num) throws IOException {
		Adddouble command = new Adddouble(key, num);
		return execute(command);
	}

	public Future<Object> ext(String name, int opts, Object key, Object value) throws IOException {
		Ext command = new Ext(name, opts, key, value);
		return execute(command);
	}

	public Future<Boolean> sync() throws IOException {
		Sync command = new Sync();
		return execute(command);
	}

	public Future<Boolean> vanish() throws IOException {
		Vanish command = new Vanish();
		return execute(command);
	}

	public Future<Boolean> copy(String path) throws IOException {
		Copy command = new Copy(path);
		return execute(command);
	}

	public Future<Boolean> restore(String path, long ts) throws IOException {
		Restore command = new Restore(path, ts);
		return execute(command);
	}
	
	public Future<Boolean> setmst(String host, int port) throws IOException {
		Setmst command = new Setmst(host, port);
		return execute(command);
	}

	public Future<Long> rnum() throws IOException {
		Rnum command = new Rnum();
		return execute(command);
	}

	public Future<Map<String, String>> stat() throws IOException {
		Stat command = new Stat();
		return execute(command);
	}

	public Future<Long> size() throws IOException {
		Size command = new Size();
		return execute(command);
	}
}
