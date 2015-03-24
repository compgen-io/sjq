package io.compgen.sjq.server;

import io.compgen.support.MonitoredThread;
import io.compgen.support.StringUtils;
import io.compgen.support.TallyValues;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ThreadedJobQueue {
	private String queueID = StringUtils.randomString(8);

	final private SJQServer server;
	final private int maxProcs;
	final private long maxMem;
	final private File tempDir;

	private int usedProcs = 0;
	private int usedMem = 0;

	private int lastId = 0;
	
	private long emptyCheck = -1;
	private long timeout = -1;
	
	public Deque<Job> pending = new ArrayDeque<Job>();
	public Map<String, RunningJob> running = new LinkedHashMap<String, RunningJob>();
	public Map<String, Job> jobs = new HashMap<String, Job>();
	
	private boolean closing = false;
	private MonitoredThread procThread = null;
		
	public ThreadedJobQueue(SJQServer server, int maxProcs, long maxMem, long timeout, String tempDir) {
		this.server = server;
		this.maxProcs = maxProcs;
		this.maxMem = maxMem;
		this.timeout = timeout;
		if (tempDir != null) {
			this.tempDir = new File(tempDir);
		} else {
			this.tempDir = null;
		}
	}

	synchronized public String getNewJobId() {
		return queueID+"."+(++lastId);
	}
	
	public void close() {
		if (!closing) {
			closing = true;
			procThread.interrupt();
			for (RunningJob run:running.values()) {
				run.kill();
			}
			server.log("Shutdown job queue...");
		}
	}

	public boolean start() {
		procThread = new MonitoredThread(new Runnable() {
			public void run() {
				while (!closing) {
					if (timeout > 0 && pending.size() == 0 && running.size() == 0) {
						if (emptyCheck == -1) {
							emptyCheck = System.currentTimeMillis();
						} else  {
							if ((System.currentTimeMillis() - emptyCheck) > timeout) {
								server.log("Timeout waiting for a job!");
								server.shutdown();
								return;
							}
						}
					}
					
					server.log("Status: " + getStatus());
					if (running.size()>0) {
						printRunningStatus();
					}

					checkJobHolds();
					Job jobToRun = findJobToRun();
		
					if (jobToRun != null) {
						runJob(jobToRun);
						emptyCheck = -1;
					} else {
						try {
							// sleep for one minute, or half the length of the timeout, which ever is shorter.
							Thread.sleep(timeout > 0 ? Math.min(60000, timeout/2) : 60000);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		});
		
		procThread.start();
		return true;
	}

	public void printRunningStatus() {
		System.err.println("-------------------------");
		for (RunningJob job: running.values()) {
			System.err.println("Running: " + job.getJob().getJobId() + " "+ StringUtils.pad(job.getJob().getName(), 12, ' ') + " " + job.getJob().getProcs() + " " + timespanToString(System.currentTimeMillis() - job.getJob().getStartTime()));
		}
		System.err.println("-------------------------");
	}
	
	public String getStatus() {
		TallyValues<String> counter = new TallyValues<String>();
		for (Job job: jobs.values()) {
			counter.incr(job.getState().getCode());
		}
		String s = "";
		for (String k: counter.keySet()) {
			if (!s.equals("")) {
				s+=" ";
			}
			s += k+":"+counter.getCount(k);
		}
		
		if (s.equals("")) {
			return "Waiting for jobs...";
		}
		return s;
	}
	
	private Job findJobToRun() {
		for (Job job: pending) {
			if (job.getState() == JobState.QUEUED) {
				if (usedProcs + job.getProcs() <= maxProcs && (maxMem < 0 || usedMem + job.getMem() < maxMem)) {
					return job;
				}
			}
		}
		return null;
	}
	
	private void checkJobHolds() {
		for (Job job: pending) {
			if (job.getState() == JobState.HOLD) {
				boolean good = true;
				for (String jobId: job.getWaitFor()) {
					Job dep = jobs.get(jobId);
					if (dep == null || dep.getState() != JobState.SUCCESS) {
						good = false;
						break;
					}
				}
				if (good) {
					job.setState(JobState.QUEUED);
				}
			}
		}
	}

	private void runJob(Job job) {
		server.log("Starting job - "+ job.getJobId());
		job.setState(JobState.RUNNING);
		job.setStartTime(System.currentTimeMillis());
		pending.remove(job);

		usedProcs += job.getProcs();
		usedMem += job.getMem();
		
		RunningJob run = new RunningJob(this, job);
		running.put(job.getJobId(), run);
		run.start();
	}

	public String addJob(Job job) {
		String jobId = getNewJobId();
		job.setJobId(jobId);
		job.setState(JobState.HOLD);
		job.setSubmitTime(System.currentTimeMillis());

		jobs.put(jobId,  job);
		pending.add(job);
		emptyCheck = -1;

		server.log("New job: "+ jobId);
		procThread.interrupt();
		return jobId;
	}

	public boolean killJob(String jobId) {
		if (running.containsKey(jobId)) {
			running.get(jobId).kill();
		}
		
		Job job = jobs.get(jobId);
		if (job != null && (job.getState() == JobState.HOLD || job.getState() == JobState.QUEUED || job.getState() == JobState.RUNNING)) {
			job.setState(JobState.KILLED);
			server.log("Job killed: " + jobId);
			return true;
		}
		return false;
	}
	
	public void jobDone(String jobId, int retcode) {
		server.log("Job done: " + jobId + " " + retcode);
		running.remove(jobId);
		Job job = jobs.get(jobId);

		if (job.getState() == JobState.RUNNING) {		
			if (retcode == 0) {
				job.setState(JobState.SUCCESS);
			} else {
				job.setState(JobState.ERROR);
			}
		}
		job.setEndTime(System.currentTimeMillis());

		usedProcs -= job.getProcs();
		usedMem -= job.getMem();
		
		procThread.interrupt();
	}
	
	public File getTempDir() {
		return tempDir;
	}

	public Job getJob(String jobId) {
		return jobs.get(jobId);
	}

	public void join() {
		while (this.procThread != null && !this.procThread.isDone()) {
			try {
				this.procThread.join(1000);
			} catch (InterruptedException e) {
			}
		}
	}
	
	public static long memStrToLong(String memVal) {
		if (memVal.toUpperCase().endsWith("G")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024 * 1024 * 1024;
		} else if (memVal.toUpperCase().endsWith("M")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024 * 1024;
		} else if (memVal.toUpperCase().endsWith("K")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024;
		}
		try {
			return Long.parseLong(memVal);
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	public static String timespanToString(long timeSpanMillis) {
		String s = "";
		
		long hours = timeSpanMillis / (60 * 60 * 1000);
		timeSpanMillis = timeSpanMillis - (hours * 60 * 60 * 1000);

		long mins = timeSpanMillis / (60 * 1000);
		timeSpanMillis = timeSpanMillis - (mins * 60 * 1000);

		long secs = timeSpanMillis / 1000;

		if (hours > 0) {
		s += hours +":";
		}

		if (mins > 10) {
			s += mins +":";
		} else if (mins > 0) {
			if (!s.equals("")) {
				s += "0";
			}
			s += mins +":";			
		} else if (mins == 0 && hours > 0) {
			if (!s.equals("")) {
				s += "00:";
			}
		}

		if (secs > 10) {
			s += secs;
		} else if (secs > 0) {
			if (!s.equals("")) {
				s += "0";
			}
			s += secs;			
		} else if (secs == 0) {
			if (!s.equals("")) {
				s += "00";
			}
		}
		
		return s;
	}
}
