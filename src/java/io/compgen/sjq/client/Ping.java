package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Command;

@Command(name="ping", desc="Find the status of a job (or server)", category="client")
public class Ping extends BaseCLI {
	@Override
	protected void process(SJQClient client) {
		try {
			if (client.ping()) {
				System.out.println("OK");
			} else {
				System.out.println("ERROR");
			}
			client.close();
		} catch (ClientException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
