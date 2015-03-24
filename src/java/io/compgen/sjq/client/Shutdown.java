package io.compgen.sjq.client;

import io.compgen.annotation.Command;

@Command(name="shutdown", desc="Shutdown a running server", category="client")
public class Shutdown extends BaseCLI {

	@Override
	protected void process(SJQClient client) {
		try {
			System.out.println(client.shutdown());
		} catch (ClientException | AuthException e) {
			// expect this... socket is closing
		}
	}
}
