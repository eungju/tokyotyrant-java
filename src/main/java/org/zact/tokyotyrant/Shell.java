package org.zact.tokyotyrant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Shell {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		TyrantClient client = new TyrantClient("dev.opencast.naver.com", 1978);
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.print("> ");
			String[] tokens = reader.readLine().split("\\s");
			String command = tokens[0];
			long s = System.currentTimeMillis();
			if ("put".equals(command)) {
				System.out.println(client.put(tokens[1], tokens[2]));
			} else if ("out".equals(command)) {
				System.out.println(client.out(tokens[1]));
			} else if ("get".equals(command)) {
				System.out.println(tokens[1] + "\t" + client.get(tokens[1]));
			} else if ("mget".equals(command)) {
				Object[] keys = Arrays.copyOfRange(tokens, 1, tokens.length);
				Map<Object, Object> values = client.mget(keys);
				for (Object key : values.keySet()) {
					System.out.println(key + "\t" + values.get(key));
				}
			} else if ("vsiz".equals(command)) {
				System.out.println(tokens[1] + "\t" + client.vsiz(tokens[1]));
			} else if ("quit".equals(command)) {
				client.close();
				break;
			}
			System.out.println("It takes " + (System.currentTimeMillis() - s) + "ms");
		}
	}
}