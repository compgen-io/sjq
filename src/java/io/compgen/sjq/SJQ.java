package io.compgen.sjq;

import io.compgen.MainBuilder;
import io.compgen.sjq.client.Status;
import io.compgen.sjq.client.Submit;
import io.compgen.sjq.server.SJQServer;

public class SJQ {
	public static void main(String[] args) throws Exception {
		new MainBuilder()
			.setProgName("sjq")
			.setHelpHeader("SJQ - Simple Job Queue\n---------------------------------------")
			.setDefaultUsage("Usage: sjq cmd [options]")
			.setHelpFooter("http://compgen.io/sjq\n"+MainBuilder.readFile("VERSION"))
			.setCategoryOrder(new String[]{"server", "client", "help"})
			.addCommand(License.class)
			.addCommand(SJQServer.class)
			.addCommand(Status.class)
			.addCommand(Submit.class)
			.findAndRun(args);
	}
}
