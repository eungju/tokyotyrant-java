package tokyotyrant;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import tokyotyrant.networking.AsynchronousNetworking;
import tokyotyrant.networking.Networking;
import tokyotyrant.protocol.Adddouble;
import tokyotyrant.protocol.Addint;
import tokyotyrant.protocol.Command;
import tokyotyrant.protocol.CommandFuture;
import tokyotyrant.protocol.Copy;
import tokyotyrant.protocol.Ext;
import tokyotyrant.protocol.Fwmkeys;
import tokyotyrant.protocol.Get;
import tokyotyrant.protocol.Iterinit;
import tokyotyrant.protocol.Iternext;
import tokyotyrant.protocol.Mget;
import tokyotyrant.protocol.Out;
import tokyotyrant.protocol.Put;
import tokyotyrant.protocol.Putcat;
import tokyotyrant.protocol.Putkeep;
import tokyotyrant.protocol.Putnr;
import tokyotyrant.protocol.Putshl;
import tokyotyrant.protocol.Restore;
import tokyotyrant.protocol.Rnum;
import tokyotyrant.protocol.Setmst;
import tokyotyrant.protocol.Size;
import tokyotyrant.protocol.Stat;
import tokyotyrant.protocol.Sync;
import tokyotyrant.protocol.Vanish;
import tokyotyrant.protocol.Vsiz;
import tokyotyrant.transcoder.SerializingTranscoder;
import tokyotyrant.transcoder.StringTranscoder;
import tokyotyrant.transcoder.Transcoder;

public class TokyoTyrantClient {
	private Transcoder keyTranscoder = new StringTranscoder();
    private Transcoder valueTranscoder = new SerializingTranscoder();
	private Networking networking;
	private long globalTimeout = 1000L;
	
    public TokyoTyrantClient(SocketAddress[] addresses) throws IOException {
    	this(addresses, new AsynchronousNetworking());
    }
	
    public TokyoTyrantClient(SocketAddress[] addresses, Networking networking) throws IOException {
		if (addresses.length == 0) {
			throw new IllegalArgumentException("Requires at least 1 node");
		}
		this.networking = networking;
		this.networking.setAddresses(addresses);
		this.networking.start();
	}

	public void dispose() {
		networking.stop();
	}
	
	public void setGlobalTimeout(long timeout) {
		this.globalTimeout = timeout;
	}

	<T> Future<T> execute(Command<T> command) throws IOException {
		return execute(command, valueTranscoder);
	}

	<T> Future<T> execute(Command<T> command, Transcoder valueTranscoder) throws IOException {
		command.setKeyTranscoder(keyTranscoder);
		command.setValueTranscoder(valueTranscoder);
		CommandFuture<T> future = new CommandFuture<T>(command, globalTimeout);
		networking.send(command);
		return future;
	}

	public Future<Boolean> put(Object key, Object value) throws IOException {
		return execute(new Put(key, value));
	}

	public Future<Boolean> put(Object key, Object value, Transcoder valueTranscoder) throws IOException {
		return execute(new Put(key, value), valueTranscoder);
	}

	public Future<Boolean> putkeep(Object key, Object value) throws IOException {
		return execute(new Putkeep(key, value));
	}

	public Future<Boolean> putkeep(Object key, Object value, Transcoder valueTranscoder) throws IOException {
		return execute(new Putkeep(key, value), valueTranscoder);
	}

	public Future<Boolean> putcat(Object key, Object value) throws IOException {
		return execute(new Putcat(key, value));
	}

	public Future<Boolean> putcat(Object key, Object value, Transcoder valueTranscoder) throws IOException {
		return execute(new Putcat(key, value), valueTranscoder);
	}

	public Future<Boolean> putshl(Object key, Object value, int width) throws IOException {
		return execute(new Putshl(key, value, width));
	}

	public Future<Boolean> putshl(Object key, Object value, int width, Transcoder valueTranscoder) throws IOException {
		return execute(new Putshl(key, value, width), valueTranscoder);
	}

	public void putnr(Object key, Object value) throws IOException {
		execute(new Putnr(key, value));
	}

	public void putnr(Object key, Object value, Transcoder valueTranscoder) throws IOException {
		execute(new Putnr(key, value), valueTranscoder);
	}

	public Future<Boolean> out(Object key) throws IOException {
		return execute(new Out(key));
	}
	
	public Future<Object> get(Object key) throws IOException {
		return execute(new Get(key));
	}
	
	public Future<Object> get(Object key, Transcoder valueTranscoder) throws IOException {
		return execute(new Get(key), valueTranscoder);
	}

	public Future<Map<Object, Object>> mget(Object[] keys) throws IOException {
		return execute(new Mget(keys));
	}

	public Future<Map<Object, Object>> mget(Object[] keys, Transcoder valueTranscoder) throws IOException {
		return execute(new Mget(keys), valueTranscoder);
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

	public Future<Object> ext(String name, Object key, Object value, int opts) throws IOException {
		return execute(new Ext(name, key, value, opts));
	}

	public Future<Object> ext(String name, Object key, Object value, int opts, Transcoder valueTranscoder) throws IOException {
		return execute(new Ext(name, key, value, opts), valueTranscoder);
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

	public Future<Long> size() throws IOException {
		return execute(new Size());
	}

	public Future<Map<String, String>> stat() throws IOException {
		return execute(new Stat());
	}
}
