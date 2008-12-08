package tokyotyrant.tester;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import tokyotyrant.RDB;

public class RDBShell extends Shell {
	private RDB db;
	
	protected void openConnection() throws IOException {
		db = new RDB();
		db.open(new InetSocketAddress(host, port));
	}
	
	protected void closeConnection() {
		db.close();
	}

	public Object repl(String input) throws Exception {
		String[] tokens = input.split("\\s");
		String command = tokens[0];
		Object result = null;
		if ("put".equals(command)) {
			result = db.put(tokens[1], tokens[2]);
		} else if ("putkeep".equals(command)) {
			result = db.putkeep(tokens[1], tokens[2]);
		} else if ("putcat".equals(command)) {
			result = db.putcat(tokens[1], tokens[2]);
		} else if ("putshl".equals(command)) {
			result = db.putshl(tokens[1], tokens[2], Integer.parseInt(tokens[3]));
		} else if ("putnr".equals(command)) {
			db.putnr(tokens[1], tokens[2]);
		} else if ("out".equals(command)) {
			result = db.out(tokens[1]);
		} else if ("get".equals(command)) {
			result = tokens[1] + "\t" + db.get(tokens[1]);
		} else if ("mget".equals(command)) {
			Object[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
			result = db.mget(keys);
		} else if ("vsiz".equals(command)) {
			result = db.vsiz(tokens[1]);
		} else if ("iterinit".equals(command)) {
			result = db.iterinit();
		} else if ("iternext".equals(command)) {
			result = db.iternext();
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
			result = keys;
		} else if ("fwmkeys".equals(command)) {
			result = db.fwmkeys(tokens[1], Integer.parseInt(tokens[2]));
		} else if ("addint".equals(command)) {
			result = db.addint(tokens[1], Integer.parseInt(tokens[2]));
		} else if ("adddouble".equals(command)) {
			result = db.adddouble(tokens[1], Double.parseDouble(tokens[2]));
		} else if ("ext".equals(command)) {
			result = db.ext(tokens[1], tokens[3], tokens[4], Integer.parseInt(tokens[2]));
		} else if ("sync".equals(command)) {
			result = db.sync();
		} else if ("vanish".equals(command)) {
			result = db.vanish();
		} else if ("copy".equals(command)) {
			result = db.copy(tokens[1]);
		} else if ("restore".equals(command)) {
			result = db.restore(tokens[1], Long.parseLong(tokens[2]));
		} else if ("setmst".equals(command)) {
			result = db.setmst(tokens[1], Integer.parseInt(tokens[2]));
		} else if ("rnum".equals(command)) {
			result = db.rnum();
		} else if ("stat".equals(command)) {
			result = db.stat();
		} else if ("size".equals(command)) {
			result = db.size();
		}
		return result;
	}
	
	public static void main(String[] args) throws IOException {
		RDBShell shell = new RDBShell();
		System.exit(shell.run(args));
	}
}
