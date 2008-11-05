package tokyotyrant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    
    public TokyoTyrantClient(String host, int port) throws IOException {
		log.info("Initializing...");
    	networking = new SynchronousNetworking(new InetSocketAddress(host, port));
    	networking.start();
		log.info("Initialized");
	}
	
	public void dispose() {
		log.info("Disposing...");
		networking.stop();
		log.info("Disposed");
	}
	
	Transcoder getTranscoder() {
		return defaultTranscoder;
	}

	void execute(Command command) throws IOException {
		command.setTranscoder(getTranscoder());
		networking.execute(command);
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
