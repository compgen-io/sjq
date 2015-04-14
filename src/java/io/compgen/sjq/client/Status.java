package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.Option;
import io.compgen.cmdline.annotation.UnnamedArg;

@Command(name="status", desc="Find the status of a job (or server)", category="client")
public class Status extends BaseCLI {
	private String jobId = null;
	private boolean verbose = false;

	@UnnamedArg(name="job-id", required=false)
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Option(charName="v", desc="Verbose status")
	public void setVerbose(boolean val) {
		this.verbose = val;
	}

	@Override
	protected void process(SJQClient client) {
		try {
			if (verbose && jobId != null) {
				client.getDetailedStatus(jobId, System.out);
			} else if (verbose) {
				client.getDetailedStatus(System.out);
			} else {
				System.out.println(client.getStatus(jobId));
			}
			client.close();
		} catch (ClientException | AuthException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
