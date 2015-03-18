package io.compgen.sjq.client;

import io.compgen.annotation.UnnamedArg;

public class Close extends BaseCLI {
	private String jobId = null;
	@UnnamedArg(name="jobid")
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Override
	protected void process(SJQClient client) {
		try {
			System.out.println(client.getStatus(jobId));
		} catch (ClientException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
