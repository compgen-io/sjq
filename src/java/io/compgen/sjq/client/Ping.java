package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Command;

@Command(name="status", desc="Find the status of a job (or server)", category="client")
public class Ping extends BaseCLI {
	@Override
	protected void process(SJQClient client) {
		try {
			System.out.println(client.ping());
			client.close();
		} catch (ClientException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
