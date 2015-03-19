package io.compgen.sjq.server;

import io.compgen.sjq.support.Counter;
import io.compgen.sjq.support.RandomUtils;
import io.compgen.sjq.support.StringUtils;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ThreadedJobQueue {
	private String queueID = RandomUtils.randomString(8);

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
	private Thread procThread = null;
	
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
		}
	}

	public boolean start() {
		procThread = new Thread() {
			public void run() {
				while (!closing) {
					if (timeout > 0 && pending.size() == 0 && running.size() == 0) {
						if (emptyCheck == -1) {
							emptyCheck = System.currentTimeMillis();
						} else  {
							if ((System.currentTimeMillis() - emptyCheck) > timeout) {
								System.err.println("Timeout waiting for a job!");
								server.shutdown();
								return;
							}
						}
					}
					
					System.err.println(new Date() + " Status: " + getStatus());
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
		};
		
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
		Counter<String> counter = new Counter<String>();
		for (Job job: jobs.values()) {
			counter.incr(job.getState().getCode());
		}
		String s = "";
		for (String k: counter.keySet()) {
			if (!s.equals("")) {
				s+=" ";
			}
			s += k+":"+counter.get(k);
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
		System.err.println("Starting job - "+ job.getJobId());
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

		System.err.println("New job: "+ jobId);
		procThread.interrupt();
		return jobId;
	}

	
	public void jobDone(String jobId, int retcode) {
		System.err.println("Job done: " + jobId);
		running.remove(jobId);
		Job job = jobs.get(jobId);

		if (retcode == 0) {
			job.setState(JobState.SUCCESS);
		} else {
			job.setState(JobState.ERROR);
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
