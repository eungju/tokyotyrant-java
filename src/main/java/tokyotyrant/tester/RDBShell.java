package tokyotyrant.tester;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tokyotyrant.RDB;

public class RDBShell extends Shell {
	private RDB db;
	private String host;
	private int port;

	protected void options(String[] args) {
		host = args[0];
		if (args.length > 1) {
			port = Integer.parseInt(args[1]);
		}
	}

	protected void openConnection() throws IOException {
		db = new RDB();
		db.open(new InetSocketAddress(host, port));
	}
	
	protected void closeConnection() {
		db.close();
	}

	public Object repl(String input) throws Exception {
		String command = command(input);
		String[] args = arguments(input);
		Object result = null;
		if ("put".equals(command)) {
			result = db.put(args[0], args[1]);
		} else if ("putkeep".equals(command)) {
			result = db.putkeep(args[0], args[1]);
		} else if ("putcat".equals(command)) {
			result = db.putcat(args[0], args[1]);
		} else if ("putshl".equals(command)) {
			result = db.putshl(args[0], args[1], Integer.parseInt(args[2]));
		} else if ("putnr".equals(command)) {
			db.putnr(args[0], args[1]);
		} else if ("out".equals(command)) {
			result = db.out(args[0]);
		} else if ("get".equals(command)) {
			result = args[0] + "\t" + db.get(args[0]);
		} else if ("mget".equals(command)) {
			Object[] keys = args;
			result = db.mget(keys);
		} else if ("vsiz".equals(command)) {
			result = db.vsiz(args[0]);
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
			result = db.fwmkeys(args[0], Integer.parseInt(args[1]));
		} else if ("addint".equals(command)) {
			result = db.addint(args[0], Integer.parseInt(args[1]));
		} else if ("adddouble".equals(command)) {
			result = db.adddouble(args[0], Double.parseDouble(args[1]));
		} else if ("ext".equals(command)) {
			result = db.ext(args[0], args[2], args[3], Integer.parseInt(args[1]));
		} else if ("sync".equals(command)) {
			result = db.sync();
		} else if ("optimize".equals(command)) {
			result = db.optimize(args[0]);
		} else if ("vanish".equals(command)) {
			result = db.vanish();
		} else if ("copy".equals(command)) {
			result = db.copy(args[0]);
		} else if ("restore".equals(command)) {
			result = db.restore(args[0], Long.parseLong(args[1]), Integer.parseInt(args[2]));
		} else if ("setmst".equals(command)) {
			result = db.setmst(args[0], Integer.parseInt(args[1]), Long.parseLong(args[2]), Integer.parseInt(args[3]));
		} else if ("rnum".equals(command)) {
			result = db.rnum();
		} else if ("size".equals(command)) {
			result = db.size();
		} else if ("stat".equals(command)) {
			result = db.stat();
		} else if ("tablePut".equals(command)) {
			Map<String, String> cols = new HashMap<String, String>();
			for (int i = 1; i < args.length; i += 2) {
				cols.put(args[i], args[i + 1]);
			}
			result = db.tablePut(args[0], cols);
		} else if ("tableGet".equals(command)) {
			result = db.tableGet(args[0]);
		} else if ("tableGenuid".equals(command)) {
			result = db.tableGenuid();
		}
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		RDBShell shell = new RDBShell();
		System.exit(shell.run(args));
	}
}
