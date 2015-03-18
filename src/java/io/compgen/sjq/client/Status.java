package io.compgen.sjq.client;

import io.compgen.annotation.Command;
import io.compgen.annotation.Option;
import io.compgen.annotation.UnnamedArg;

@Command(name="status", desc="Find the status of a job (or server)", category="client")
public class Status extends BaseCLI {
	private String jobId = null;
	private boolean verbose = false;

	@UnnamedArg(name="jobid")
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Option(charName="v")
	public void setVerbose(boolean val) {
		this.verbose = val;
	}

	@Override
	protected void process(SJQClient client) {
		try {
			if (verbose && jobId != null) {
				client.getDetailedStatus(jobId, System.out);
				
			} else {
				System.out.println(client.getStatus(jobId));
			}
		} catch (ClientException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
