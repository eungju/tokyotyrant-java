package tokyotyrant.tester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public abstract class Shell {
	protected BufferedReader stdin;
	protected PrintWriter stdout;
	protected PrintWriter stderr;
	
	protected String host;
	protected int port = 1978;
	
	public Shell() {
		stdin = new BufferedReader(new InputStreamReader(System.in));
		stdout = new PrintWriter(System.out);
		stderr = new PrintWriter(System.err);
	}
	
	protected String prompt() throws IOException {
		stdout.print(">>> ");
		stdout.flush();
		return stdin.readLine();
	}
	
	protected void options(String[] args) {
		host = args[0];
		if (args.length > 1) {
			port = Integer.parseInt(args[1]);
		}
	}
	
	public int run(String[] args) throws Exception {
		options(args);
		openConnection();
		while (true) {
			try {
				String input = prompt();
				if ("quit".equals(input)) {
					break;
				}
				long s = System.currentTimeMillis();
				Object result = repl(input);
				long e = System.currentTimeMillis();
				stdout.println("(" + (e - s) + "ms)" + "\t" + result);
			} catch (Exception e) {
				e.printStackTrace(stderr);
			}
		}
		closeConnection();
		return 0;
	}

	protected abstract void openConnection() throws Exception;

	protected abstract void closeConnection();

	public abstract Object repl(String input) throws Exception;
}
