package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.UnnamedArg;

@Command(name="kill", desc="Cancel a queued or running job", category="client")
public class Kill extends BaseCLI {
	private String jobId = null;

	@UnnamedArg(name="jobid")
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Override
	protected void process(SJQClient client) {
		try {
			System.out.println(client.killJob(jobId));
			client.close();
		} catch (ClientException | AuthException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
