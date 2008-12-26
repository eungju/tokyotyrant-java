package tokyotyrant;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import tokyotyrant.networking.ActiveStandbyNodeLocator;
import tokyotyrant.networking.NioNetworking;
import tokyotyrant.networking.Networking;
import tokyotyrant.networking.ServerNode;
import tokyotyrant.protocol.Adddouble;
import tokyotyrant.protocol.Addint;
import tokyotyrant.protocol.Command;
import tokyotyrant.protocol.CommandFuture;
import tokyotyrant.protocol.Ext;
import tokyotyrant.protocol.Fwmkeys;
import tokyotyrant.protocol.Get;
import tokyotyrant.protocol.Mget;
import tokyotyrant.protocol.Out;
import tokyotyrant.protocol.Put;
import tokyotyrant.protocol.Putcat;
import tokyotyrant.protocol.Putkeep;
import tokyotyrant.protocol.Putnr;
import tokyotyrant.protocol.Putshl;
import tokyotyrant.protocol.Rnum;
import tokyotyrant.protocol.Size;
import tokyotyrant.protocol.Stat;
import tokyotyrant.protocol.Sync;
import tokyotyrant.protocol.Vanish;
import tokyotyrant.protocol.Vsiz;
import tokyotyrant.transcoder.SerializingTranscoder;
import tokyotyrant.transcoder.StringTranscoder;
import tokyotyrant.transcoder.Transcoder;

/**
 * Multiple Rs DB. Replicated, Reliable, Responsive, Remote, etc.
 * API is similar to {@link RDB}, but {@link MRDB} returns {@link Future}.
 */
public class MRDB {
	private Transcoder keyTranscoder = new StringTranscoder();
    private Transcoder valueTranscoder = new SerializingTranscoder();
	private long globalTimeout = 1000L;
	private Networking networking;
	
    public MRDB(URI[] addresses) throws Exception {
    	this(addresses, new NioNetworking(new ActiveStandbyNodeLocator()));
    }
	
    public MRDB(URI[] addresses, Networking networking) throws Exception {
		if (addresses.length == 0) {
			throw new IllegalArgumentException("Requires at least 1 node");
		}
		this.networking = networking;
		this.networking.initialize(addresses);
		this.networking.start();
	}

	public void dispose() {
		networking.stop();
	}
	
	public void setGlobalTimeout(long timeout) {
		this.globalTimeout = timeout;
	}

	protected <T> CommandFuture<T> execute(Command<T> command) {
		return execute(command, valueTranscoder);
	}

	protected <T> CommandFuture<T> execute(Command<T> command, Transcoder valueTranscoder) {
		command.setKeyTranscoder(keyTranscoder);
		command.setValueTranscoder(valueTranscoder);
		CommandFuture<T> future = new CommandFuture<T>(command, globalTimeout);
		networking.send(command);
		return future;
	}

	protected <T> CommandFuture<T> execute(ServerNode node, Command<T> command) {
		command.setKeyTranscoder(keyTranscoder);
		command.setValueTranscoder(valueTranscoder);
		CommandFuture<T> future = new CommandFuture<T>(command, globalTimeout);
		networking.send(node, command);
		return future;
	}
	
	public <T> T await(Future<T> future) throws RuntimeException {
		try {
			return future.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Exception while executing", e);
		}
	}

	public <T> T await(Future<T> future, long timeout, TimeUnit unit) throws RuntimeException {
		try {
			return future.get(timeout, unit);
		} catch (TimeoutException e) {
			throw new RuntimeException("Timeout", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Exception while executing", e);
		}
	}

	public Future<Boolean> put(Object key, Object value) {
		return execute(new Put(key, value));
	}

	public Future<Boolean> put(Object key, Object value, Transcoder valueTranscoder) {
		return execute(new Put(key, value), valueTranscoder);
	}

	public Future<Boolean> putkeep(Object key, Object value) {
		return execute(new Putkeep(key, value));
	}

	public Future<Boolean> putkeep(Object key, Object value, Transcoder valueTranscoder) {
		return execute(new Putkeep(key, value), valueTranscoder);
	}

	public Future<Boolean> putcat(Object key, Object value) {
		return execute(new Putcat(key, value));
	}

	public Future<Boolean> putcat(Object key, Object value, Transcoder valueTranscoder) {
		return execute(new Putcat(key, value), valueTranscoder);
	}

	public Future<Boolean> putshl(Object key, Object value, int width) {
		return execute(new Putshl(key, value, width));
	}

	public Future<Boolean> putshl(Object key, Object value, int width, Transcoder valueTranscoder) {
		return execute(new Putshl(key, value, width), valueTranscoder);
	}

	public void putnr(Object key, Object value) {
		execute(new Putnr(key, value));
	}

	public void putnr(Object key, Object value, Transcoder valueTranscoder) {
		execute(new Putnr(key, value), valueTranscoder);
	}

	public Future<Boolean> out(Object key) {
		return execute(new Out(key));
	}
	
	public Future<Object> get(Object key) {
		return execute(new Get(key));
	}
	
	public Future<Object> get(Object key, Transcoder valueTranscoder) {
		return execute(new Get(key), valueTranscoder);
	}

	public Future<Map<Object, Object>> mget(Object[] keys) {
		return execute(new Mget(keys));
	}

	public Future<Map<Object, Object>> mget(Object[] keys, Transcoder valueTranscoder) {
		return execute(new Mget(keys), valueTranscoder);
	}

	public Future<Integer> vsiz(Object key) {
		return execute(new Vsiz(key));
	}

	public Future<List<Object>> fwmkeys(Object prefix, int max) {
		return execute(new Fwmkeys(prefix, max));
	}

	public Future<Integer> addint(Object key, int num) {
		return execute(new Addint(key, num));
	}

	public Future<Double> adddouble(Object key, double num) {
		return execute(new Adddouble(key, num));
	}

	public Future<Object> ext(String name, Object key, Object value, int opts) {
		return execute(new Ext(name, key, value, opts));
	}

	public Future<Object> ext(String name, Object key, Object value, int opts, Transcoder valueTranscoder) {
		return execute(new Ext(name, key, value, opts), valueTranscoder);
	}

	public Future<Boolean> sync() {
		return execute(new Sync());
	}

	public Future<Boolean> vanish() {
		return execute(new Vanish());
	}

	public Future<Long> rnum() {
		return execute(new Rnum());
	}

	public Future<Long> size() {
		return execute(new Size());
	}

	public Map<URI, Map<String, String>> stat() {
		Map<ServerNode, Future<Map<String, String>>> futures = new HashMap<ServerNode, Future<Map<String, String>>>();
		for (ServerNode each : networking.getNodeLocator().getAll()) {
			futures.put(each, execute(new Stat()));
		}
		Map<URI, Map<String, String>> result = new HashMap<URI, Map<String, String>>();
		for (ServerNode each : networking.getNodeLocator().getAll()) {
			result.put(each.getAddress(), await(futures.get(each)));
		}
		return result;
	}
}
