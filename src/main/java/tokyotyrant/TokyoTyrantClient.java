package tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
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
		return execute(new Put(key, value));
	}

	public Future<Boolean> putkeep(Object key, Object value) throws IOException {
		return execute(new Putkeep(key, value));
	}

	public Future<Boolean> putcat(Object key, Object value) throws IOException {
		return execute(new Putcat(key, value));
	}

	public Future<Boolean> putrtt(Object key, Object value, int width) throws IOException {
		return execute(new Putrtt(key, value, width));
	}

	public void putnr(Object key, Object value) throws IOException {
		execute(new Putnr(key, value));
	}

	public Future<Boolean> out(Object key) throws IOException {
		return execute(new Out(key));
	}
	
	public Future<Object> get(Object key) throws IOException {
		return execute(new Get(key));
	}
	
	public Future<Map<Object, Object>> mget(Object... keys) throws IOException {
		return execute(new Mget(keys));
	}

	public Future<Integer> vsiz(Object key) throws IOException {
		return execute(new Vsiz(key));
	}

	public Future<Boolean> iterinit() throws IOException {
		return execute(new Iterinit());
	}
	
	public Future<Object> iternext() throws IOException {
		return execute(new Iternext());
	}

	public Future<List<Object>> fwmkeys(Object prefix, int max) throws IOException {
		return execute(new Fwmkeys(prefix, max));
	}

	public Future<Integer> addint(Object key, int num) throws IOException {
		return execute(new Addint(key, num));
	}

	public Future<Double> adddouble(Object key, double num) throws IOException {
		return execute(new Adddouble(key, num));
	}

	public Future<Object> ext(String name, int opts, Object key, Object value) throws IOException {
		return execute(new Ext(name, opts, key, value));
	}

	public Future<Boolean> sync() throws IOException {
		return execute(new Sync());
	}

	public Future<Boolean> vanish() throws IOException {
		return execute(new Vanish());
	}

	public Future<Boolean> copy(String path) throws IOException {
		return execute(new Copy(path));
	}

	public Future<Boolean> restore(String path, long ts) throws IOException {
		return execute(new Restore(path, ts));
	}
	
	public Future<Boolean> setmst(String host, int port) throws IOException {
		return execute(new Setmst(host, port));
	}

	public Future<Long> rnum() throws IOException {
		return execute(new Rnum());
	}

	public Future<Map<String, String>> stat() throws IOException {
		return execute(new Stat());
	}

	public Future<Long> size() throws IOException {
		return execute(new Size());
	}
}
