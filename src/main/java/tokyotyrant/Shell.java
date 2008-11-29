package tokyotyrant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;

public class Shell {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		TokyoTyrantClient client = new TokyoTyrantClient(args[0], Integer.parseInt(args[1]));
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.print("> ");
			String[] tokens = reader.readLine().split("\\s");
			String command = tokens[0];
			long s = System.currentTimeMillis();
			if ("put".equals(command)) {
				System.out.println(client.put(tokens[1], tokens[2]).get());
			} else if ("putkeep".equals(command)) {
				System.out.println(client.putkeep(tokens[1], tokens[2]).get());
			} else if ("putcat".equals(command)) {
				System.out.println(client.putcat(tokens[1], tokens[2]).get());
			} else if ("putrtt".equals(command)) {
				System.out.println(client.putrtt(tokens[1], tokens[2], Integer.parseInt(tokens[3])).get());
			} else if ("putnr".equals(command)) {
				client.putnr(tokens[1], tokens[2]);
			} else if ("out".equals(command)) {
				System.out.println(client.out(tokens[1]).get());
			} else if ("get".equals(command)) {
				System.out.println(tokens[1] + "\t" + client.get(tokens[1]).get());
			} else if ("mget".equals(command)) {
				Object[] keys = ArrayUtils.subarray(tokens, 1, tokens.length);
				Map<Object, Object> values = client.mget(keys).get();
				for (Object key : values.keySet()) {
					System.out.println(key + "\t" + values.get(key));
				}
			} else if ("vsiz".equals(command)) {
				System.out.println(client.vsiz(tokens[1]).get());
			} else if ("list".equals(command)) {
				System.out.println(client.list());
			} else if ("fwmkeys".equals(command)) {
				System.out.println(client.fwmkeys(tokens[1], Integer.parseInt(tokens[2])).get());
			} else if ("addint".equals(command)) {
				System.out.println(client.addint(tokens[1], Integer.parseInt(tokens[2])).get());
			} else if ("adddouble".equals(command)) {
				System.out.println(client.adddouble(tokens[1], Double.parseDouble(tokens[2])).get());
			} else if ("ext".equals(command)) {
				System.out.println(client.ext(tokens[1], Integer.parseInt(tokens[2]), tokens[3], tokens[4]).get());
			} else if ("sync".equals(command)) {
				System.out.println(client.sync().get());
			} else if ("vanish".equals(command)) {
				System.out.println(client.vanish().get());
			} else if ("copy".equals(command)) {
				System.out.println(client.copy(tokens[1]).get());
			} else if ("restore".equals(command)) {
				System.out.println(client.restore(tokens[1], Long.parseLong(tokens[2])).get());
			} else if ("setmst".equals(command)) {
				System.out.println(client.setmst(tokens[1], Integer.parseInt(tokens[2])).get());
			} else if ("rnum".equals(command)) {
				System.out.println(client.rnum().get());
			} else if ("stat".equals(command)) {
				System.out.println(client.stat().get());
			} else if ("size".equals(command)) {
				System.out.println(client.size().get());
			} else if ("quit".equals(command)) {
				client.dispose();
				break;
			}
			System.out.println("It takes " + (System.currentTimeMillis() - s) + "ms");
		}
	}
}