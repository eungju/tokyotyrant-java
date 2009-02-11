package tokyotyrant.tester;

import java.net.URI;

import org.apache.commons.lang.ArrayUtils;

import tokyotyrant.MRDB;

public class MRDBShell extends Shell {
	private MRDB db;
	private URI[] addresses;
	
	protected void options(String[] args) {
		addresses = new URI[args.length];
		int i = 0;
		for (String each : args) {
			addresses[i++] = URI.create(each);
		}
	}

	protected void openConnection() throws Exception {
		db = new MRDB();
		db.open(addresses);
	}

	protected void closeConnection() {
		db.close();
	}
	
	public Object repl(String input) throws Exception {
		String[] tokens = input.split("\\s");
		String command = tokens[0];
		Object result = null;
		if ("put".equals(command)) {
			result = db.put(tokens[1], tokens[2]).get();
		} else if ("putkeep".equals(command)) {
			result = db.putkeep(tokens[1], tokens[2]).get();
		} else if ("putcat".equals(command)) {
			result = db.putcat(tokens[1], tokens[2]).get();
		} else if ("putshl".equals(command)) {
			result = db.putshl(tokens[1], tokens[2], Integer.parseInt(tokens[3])).get();
		} else if ("putnr".equals(command)) {
			db.putnr(tokens[1], tokens[2]);
		} else if ("out".equals(command)) {
			result = db.out(tokens[1]).get();
		} else if ("get".equals(command)) {
			result = tokens[1] + "\t" + db.get(tokens[1]).get();
		} else if ("mget".equals(command)) {
			Object[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
			result = db.mget(keys).get();
		} else if ("vsiz".equals(command)) {
			result = db.vsiz(tokens[1]).get();
		} else if ("list".equals(command)) {
			result = db.fwmkeys("", Integer.MAX_VALUE).get();
		} else if ("fwmkeys".equals(command)) {
			result = db.fwmkeys(tokens[1], Integer.parseInt(tokens[2])).get();
		} else if ("addint".equals(command)) {
			result = db.addint(tokens[1], Integer.parseInt(tokens[2])).get();
		} else if ("adddouble".equals(command)) {
			result = db.adddouble(tokens[1], Double.parseDouble(tokens[2])).get();
		} else if ("ext".equals(command)) {
			result = db.ext(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4])).get();
		} else if ("sync".equals(command)) {
			result = db.sync().get();
		} else if ("vanish".equals(command)) {
			result = db.vanish().get();
		} else if ("rnum".equals(command)) {
			result = db.rnum().get();
		} else if ("size".equals(command)) {
			result = db.size().get();
		} else if ("stat".equals(command)) {
			result = db.stat();
		}
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		MRDBShell shell = new MRDBShell();
		System.exit(shell.run(args));
	}
}