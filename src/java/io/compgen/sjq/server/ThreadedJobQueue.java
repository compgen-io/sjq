package io.compgen.sjq.server;

import io.compgen.sjq.support.Counter;
import io.compgen.sjq.support.RandomUtils;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
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
	public Map<String, RunningJob> running = new HashMap<String, RunningJob>();
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
					System.err.println("Status: " + getStatus());
					if (timeout > 0 && pending.size() == 0 && running.size() == 0) {
						if (emptyCheck == -1) {
							emptyCheck = System.currentTimeMillis();
						} else  {
							if ((System.currentTimeMillis() - emptyCheck) > timeout) {
								// idle timeout!
								server.shutdown();
								return;
							}
						}
					}
					
					checkJobHolds();
					Job jobToRun = findJobToRun();
		
					if (jobToRun != null) {
						runJob(jobToRun);
						emptyCheck = -1;
					} else {
						try {
							Thread.sleep(60000);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		};
		
		procThread.start();
		return true;
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
		procThread.interrupt();
	}
	
	public File getTempDir() {
		return tempDir;
	}

	public Job getJob(String jobId) {
		return jobs.get(jobId);
	}
}
