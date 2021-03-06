package io.compgen.sjq.server;

import io.compgen.common.MonitoredThread;
import io.compgen.common.StringUtils;
import io.compgen.common.TallyValues;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
	public Map<String, Job> jobs = new LinkedHashMap<String, Job>();
	
	private boolean closing = false;
	private MonitoredThread procThread = null;
	
	private Lock lock = new ReentrantLock();
	
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

	public String getNewJobId() {
		lock.lock();
		String jobid = queueID+"."+(++lastId);
		lock.unlock();
		return jobid;
	}
	
	public void close() {
		if (!closing) {
			lock.lock();
			closing = true;
			procThread.interrupt();
			List<String> runningJobIds = new ArrayList<String>();
			for (RunningJob runningjob:running.values()) {
				server.log("Currently running job: "+runningjob.getJob().getJobId());
				runningJobIds.add(runningjob.getJob().getJobId());
			}
			for (String jobId: runningJobIds) {
				killJob(jobId);
			}
			server.log("Shutdown job queue...");
			lock.unlock();
		}
	}

	public boolean start() {
		lock.lock();
		if (procThread != null) {
			lock.unlock();
			return false;
		}
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
		lock.unlock();
		return true;
	}

	public void printRunningStatus() {
		System.err.println("-------------------------");
		lock.lock();
		for (RunningJob job: running.values()) {
			System.err.println("Running: " + job.getJob().getJobId() + " "+ StringUtils.pad(job.getJob().getName(), 12, ' ') + " " + job.getJob().getProcs() + " " + timespanToString(System.currentTimeMillis() - job.getJob().getStartTime()));
		}
		lock.unlock();
		System.err.println("-------------------------");
	}
	
	public String getServerStatus() {
		String out = "";
		lock.lock();
		for (Job job: server.getQueue().jobs.values()) {
			if (!out.equals("")) {
				out += "\n";
			}
			List<String> outs = new ArrayList<String>();
			outs.add(job.getJobId());
			outs.add(job.getName());
			outs.add(job.getState().getCode());
			outs.add(""+ThreadedJobQueue.timestampToString(job.getSubmitTime()));
			if (job.getState() == JobState.RUNNING) {
				outs.add(""+ThreadedJobQueue.timestampToString(job.getStartTime()));
				outs.add(""+ThreadedJobQueue.timespanToString(System.currentTimeMillis() - job.getStartTime()));
			} else if (job.getState() == JobState.SUCCESS || job.getState() == JobState.ERROR) {
				outs.add(""+ThreadedJobQueue.timestampToString(job.getStartTime()));
				outs.add(""+ThreadedJobQueue.timestampToString(job.getEndTime()));
				outs.add(""+job.getRetCode());
				outs.add(""+ThreadedJobQueue.timespanToString(job.getEndTime() - job.getStartTime()));
			} else {
				outs.add(""+ThreadedJobQueue.timespanToString(System.currentTimeMillis() - job.getSubmitTime()));
			}
			out += StringUtils.join("\t", outs);
		}
		lock.unlock();
		return out;
	}
	
	public String getStatus() {
		TallyValues<String> counter = new TallyValues<String>();
		lock.lock();
		for (Job job: jobs.values()) {
			counter.incr(job.getState().getCode());
		}
		lock.unlock();

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
		lock.lock();
		for (Job job: pending) {
			if (job.getState() == JobState.QUEUED) {
				if (usedProcs + job.getProcs() <= maxProcs && (maxMem < 0 || usedMem + job.getMem() < maxMem)) {
					lock.unlock();
					return job;
				}
			}
		}
		lock.unlock();
		return null;
	}
	
	private void checkJobHolds() {
		lock.lock();
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
		lock.unlock();
	}

	private void runJob(Job job) {
		if (closing) {
			return;
		}
		lock.lock();
		server.log("Starting job - "+ job.getJobId());
		job.setState(JobState.RUNNING);
		job.setStartTime(System.currentTimeMillis());
		pending.remove(job);

		usedProcs += job.getProcs();
		usedMem += job.getMem();
		
		RunningJob run = new RunningJob(this, job);
		running.put(job.getJobId(), run);
		lock.unlock();
		run.start();
	}

	public String addJob(Job job) {
		String jobId = getNewJobId();
		job.setJobId(jobId);
		job.setState(JobState.HOLD);
		job.setSubmitTime(System.currentTimeMillis());

		lock.lock();
		jobs.put(jobId,  job);
		pending.add(job);
		lock.unlock();
		emptyCheck = -1;

		server.log("New job: "+ jobId+ " "+job.getName());
		procThread.interrupt();
		return jobId;
	}

	public boolean killJob(String jobId) {
		Job job = jobs.get(jobId);
		
		if (job == null) {
			return false;
		}
		
		lock.lock();
		if (running.containsKey(jobId)) {
			server.log("Killing job: " + jobId);
			running.get(jobId).kill();
			job.setState(JobState.KILLED);
			server.log("Job killed: " + jobId);
			lock.unlock();
			return true;
		}
		
		if (job != null && (job.getState() == JobState.HOLD || job.getState() == JobState.QUEUED)) {
			job.setState(JobState.KILLED);
			server.log("Job killed: " + jobId);
			lock.unlock();
			return true;
		}
		lock.unlock();
		return false;
	}
	
	public void jobDone(String jobId, int retcode) {
		server.log("Job done: " + jobId + " " + retcode);
		lock.lock();
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
		lock.unlock();

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

    public static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static String timestampToString(long ts) {
	    return dateFormat.format(new Date(ts));
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

		if (mins > 9) {
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
		} else {
			s += "0:";
		}

		if (secs > 9) {
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

	public SJQServer getServer() {
		return server;
	}

	public boolean releaseJob(String jobId) {
		Job job = jobs.get(jobId);
		
		if (job == null) {
			return false;
		}
		
		lock.lock();
		if (job != null && (job.getState() == JobState.USERHOLD)) {
			job.setState(JobState.HOLD);
			server.log("Job released from user-hold: " + jobId);
			procThread.interrupt();
			lock.unlock();
			return true;
		}
		lock.unlock();
		return false;
	}
}
