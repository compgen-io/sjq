package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.Option;
import io.compgen.cmdline.annotation.UnnamedArg;
import io.compgen.common.ListBuilder;
import io.compgen.common.StringLineReader;
import io.compgen.common.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Command(name="submit", desc="Submit a new job", category="client", doc="In addition to setting command-line parameters, ")
public class Submit extends BaseCLI {
	private String name=null;
	private String script=null;
	
	private int procs = -1;
	private String mem=null;
	private boolean userHold=false;
	private Map<String, String> env = null;
	
	private String cwd=null;
	private String stdout=null;
	private String stderr=null;

	private String deps="";
	
	@UnnamedArg(name="script")
	public void setScript(String script) {
		this.script = script;
	}

	@Option(name="name", charName="n", desc="Job name (default: script name)")
	public void setName(String name) {
		this.name = name;
	}

	@Option(name="procs", charName="N", desc="Required processors (default: 1)")
	public void setProcs(int procs) {
		this.procs = procs;
	}

	@Option(name="mem", charName="m", desc="Max memory, ex: '2G' (default: not used)")
	public void setMem(String mem) {
		this.mem = mem;
	}

	@Option(name="cwd", desc="Job working directory (default: current directory)")
	public void setCwd(String cwd) {
		this.cwd = new File(cwd).getAbsolutePath();
	}

	@Option(name="hold", desc="Set a user-hold on job")
	public void setUserHold(boolean userHold) {
		this.userHold = userHold;
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
	protected void process(SJQClient client) throws IOException {
		if (name == null) {
			if (script.equals("-")) {
				name = "stdin";
			} else {
				name = new File(script).getName();
			}
		}
		
		for (String line: new StringLineReader(script)) {
			if (line.startsWith("#$")) {
				line = StringUtils.strip(line).substring(2);
				String[] cmds = line.split(" ", 2);
				switch (cmds[0]) {
				case "N":
					if (procs == -1) {
						setProcs(Integer.parseInt(cmds[1]));
					}
					break;
				case "n":
					if (name == null) {
						setName(cmds[1]);
					}
					break;
				case "m":
					if (mem == null) {
						setMem(cmds[1]);
					}
					break;
				case "cwd":
					if (cwd == null) {
						setCwd(cmds[1]);
					}
					break;
				case "env":
					if (env == null) {
						setEnv(true);
					}
				case "hold":
					if (!userHold) {
						setUserHold(true);
					}
					break;
				case "e":
					if (stderr == null) {
						setStderr(cmds[1]);
					}
					break;
				case "o":
					if (stdout == null) {
						setStdout(cmds[1]);
					}
					break;
				case "deps":
					if (deps == null) {
						setDeps(cmds[1]);
					}
					break;
				}
			}
		}
		
		try {
			String jobid = client.submitJob(name, StringUtils.readFile(script), procs, mem, stderr, stdout, cwd, env, ListBuilder.build(deps.split(",")), userHold);
			System.out.println(jobid);
			client.close();
		} catch (ClientException | IOException | AuthException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
