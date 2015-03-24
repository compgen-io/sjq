package io.compgen.sjq.server;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Job {
	private String jobId = null;
	private JobState state = JobState.HOLD;

	private List<String> waitFor = new ArrayList<String>();
	
	private String name;
	private String body;
	
	private String cwd = null;
	private String stdout = null;
	private String stderr = null;
	private Map<String,String> env = null;
	
	private int procs = 1;
	private long mem = -1;
	
	private long submitTime = -1;
	private long startTime = -1;
	private long endTime = -1;
	
	private int retCode;
	
	public Job(String name) {
		this.name = name;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getStdout() {
		return stdout;
	}
	

	public void setStdout(String stdout) {
		this.stdout = stdout;
	}

	public String getStderr() {
		return stderr;
	}

	public void setStderr(String stderr) {
		this.stderr = stderr;
	}

	public int getProcs() {
		return procs;
	}

	public void setProcs(int procs) {
		if (procs < 1) {
			this.procs = 1;
		} else {
			this.procs = procs;
		}
	}

	public long getMem() {
		return mem;
	}

	public void setMem(long mem) {
		if (mem > 0) {
			this.mem = mem;
		} else {
			this.mem=-1;
		}
	}
	public void setMem(String mem) {
		setMem(ThreadedJobQueue.memStrToLong(mem));
	}
	
	public void addWaitForJob(String jobId) {
		this.waitFor.add(jobId);
	}
	public List<String> getWaitFor() {
		return Collections.unmodifiableList(this.waitFor);
	}

	public JobState getState() {
		return state;
	}

	public void setState(JobState state) {
		this.state = state;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public int getRetCode() {
		return retCode;
	}

	public void setRetCode(int retCode) {
		this.retCode = retCode;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public void setWaitFor(List<String> waitFor) {
		this.waitFor = waitFor;
	}

	public String getCwd() {
		return cwd;
	}
	public String getCwdDefault() {
		return (cwd != null) ? cwd : Paths.get("").toAbsolutePath().toString();
	}

	public void setCwd(String cwd) {
		this.cwd = cwd;
	}

	public void setSubmitTime(long submitTime) {
		this.submitTime = submitTime;
	}

	public void addEnv(String k, String val) {
		if (env == null) {
			env = new HashMap<String, String>();
		}
		env.put(k,val);
	}
	
	public Map<String, String> getEnv() {
		return env;
	}

	public void setEnv(Map<String, String> env) {
		this.env = env;
	}
}

