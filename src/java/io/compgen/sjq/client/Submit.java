package io.compgen.sjq.client;

import io.compgen.annotation.Command;
import io.compgen.annotation.Option;
import io.compgen.annotation.UnnamedArg;
import io.compgen.sjq.support.ListUtils;
import io.compgen.sjq.support.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Command(name="submit", desc="Submit a new job", category="client")
public class Submit extends BaseCLI {
	private String name=null;
	private String script=null;
	
	private int procs = 1;
	private String mem=null;
	private Map<String, String> env = null;
	
	private String cwd=null;
	private String stdout=null;
	private String stderr=null;

	private String deps=null;
	
	@UnnamedArg(name="script", required=true)
	public void setScript(String script) {
		this.script = script;
	}

	@Option(name="name", charName="n", desc="Job name (default: script name)")
	public void setName(String name) {
		this.name = name;
	}

	@Option(name="procs", charName="p", desc="Processors (default: 1)")
	public void setProcs(int procs) {
		this.procs = procs;
	}

	@Option(name="mem", charName="m", desc="Max memory, ex: 2G (default: not used)")
	public void setMem(String mem) {
		this.mem = mem;
	}

	@Option(name="cwd", desc="Job working directory (default: current directory)")
	public void setCwd(String cwd) {
		this.cwd = cwd;
	}

	@Option(name="stderr", charName="e", desc="Redirect stderr to file (default: jobname.jobid.stderr)")
	public void setStderr(String stderr) {
		this.stderr = stderr;
	}

	@Option(name="stdout", charName="o", desc="Redirect stdout to file (default: jobname.jobid.stdout)")
	public void setStdout(String stdout) {
		this.stdout = stdout;
	}

	@Option(name="deps", desc="Job dependencies (comma-delimited list)")
	public void setDeps(String deps) {
		this.deps = deps;
	}

	@Option(name="env", desc="Capture current environment")
	public void setEnv(boolean val) {
		if (val) {
			this.env = System.getenv();
		}
	}
	
	@Override
	protected void process(SJQClient client) {
		if (name == null) {
			if (script.equals("-")) {
				name = "stdin";
			} else {
				name = new File(script).getName();
			}
		}
		
		try {
			String jobid = client.submitJob(name, StringUtils.readFile(script), procs, mem, stderr, stdout, cwd, env, ListUtils.build(deps.split(",")));
			System.out.println(jobid);
		} catch (ClientException | IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
