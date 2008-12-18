package tokyotyrant.tester;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.lang.ArrayUtils;

import tokyotyrant.TokyoTyrantClient;

public class TokyoTyrantClientShell extends Shell {
	private TokyoTyrantClient client;

	protected void openConnection() throws IOException {
		client = new TokyoTyrantClient(new InetSocketAddress[] { new InetSocketAddress(host, port) });
	}

	protected void closeConnection() {
		client.dispose();
	}
	
	public Object repl(String input) throws Exception {
		String[] tokens = input.split("\\s");
		String command = tokens[0];
		Object result = null;
		if ("put".equals(command)) {
			result = client.put(tokens[1], tokens[2]).get();
		} else if ("putkeep".equals(command)) {
			result = client.putkeep(tokens[1], tokens[2]).get();
		} else if ("putcat".equals(command)) {
			result = client.putcat(tokens[1], tokens[2]).get();
		} else if ("putshl".equals(command)) {
			result = client.putshl(tokens[1], tokens[2], Integer.parseInt(tokens[3])).get();
		} else if ("putnr".equals(command)) {
			client.putnr(tokens[1], tokens[2]);
		} else if ("out".equals(command)) {
			result = client.out(tokens[1]).get();
		} else if ("get".equals(command)) {
			result = tokens[1] + "\t" + client.get(tokens[1]).get();
		} else if ("mget".equals(command)) {
			Object[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
			result = client.mget(keys).get();
		} else if ("vsiz".equals(command)) {
			result = client.vsiz(tokens[1]).get();
		} else if ("list".equals(command)) {
			result = client.fwmkeys("", Integer.MAX_VALUE).get();
		} else if ("fwmkeys".equals(command)) {
			result = client.fwmkeys(tokens[1], Integer.parseInt(tokens[2])).get();
		} else if ("addint".equals(command)) {
			result = client.addint(tokens[1], Integer.parseInt(tokens[2])).get();
		} else if ("adddouble".equals(command)) {
			result = client.adddouble(tokens[1], Double.parseDouble(tokens[2])).get();
		} else if ("ext".equals(command)) {
			result = client.ext(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4])).get();
		} else if ("sync".equals(command)) {
			result = client.sync().get();
		} else if ("vanish".equals(command)) {
			result = client.vanish().get();
		} else if ("rnum".equals(command)) {
			result = client.rnum().get();
		} else if ("size".equals(command)) {
			result = client.size().get();
		} else if ("stat".equals(command)) {
			result = client.stat();
		}
		return result;
	}
	
	public static void main(String[] args) throws IOException {
		TokyoTyrantClientShell shell = new TokyoTyrantClientShell();
		System.exit(shell.run(args));
	}
}