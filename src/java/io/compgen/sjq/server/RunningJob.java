package io.compgen.sjq.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class RunningJob {
	final private Job job;
	final private ThreadedJobQueue queue;
	private Thread runningThread = null;
	private Process proc = null;
	
	public RunningJob(ThreadedJobQueue queue, Job job) {
		this.queue = queue;
		this.job = job;
	}

	public Job getJob() {
		return job;
	}
	
	public void start() {
		final File script;
		try {
			script = File.createTempFile("sjq-"+job.getJobId(), ".sh", queue.getTempDir());
			script.createNewFile();
			script.setExecutable(false, false);
			script.setReadable(false, false);
			script.setWritable(false, false);
			script.setExecutable(true, true);
			script.setReadable(true, true);
			script.setWritable(true, true);
			OutputStream os = new FileOutputStream(script);
			os.write(job.getBody().getBytes());
			os.close();
		} catch (IOException e) {
			queue.jobDone(job.getJobId(), 1000);
			return;
		}		

		runningThread = new Thread() {
			public void run() {
				int retcode = 100;

				File stdout;
				if (job.getStdout() != null) { 
					stdout = new File(job.getStdout());
				
					if (stdout.isDirectory()) {
						stdout = new File(stdout, job.getName()+"."+job.getJobId()+".stdout");
					}
				} else {
					stdout = new File(job.getName()+"."+job.getJobId()+".stdout");
				}
				
				File stderr;
				if (job.getStderr() != null) { 
					stderr = new File(job.getStderr());
				
					if (stderr.isDirectory()) {
						stderr = new File(stderr, job.getName()+"."+job.getJobId()+".stderr");
					}
				} else {
					stderr = new File(job.getName()+"."+job.getJobId()+".stderr");
				}
				
				
				try {
					ProcessBuilder pb = new ProcessBuilder()
						.command(script.getAbsolutePath())
						.directory(new File(job.getCwdDefault()))
						.redirectOutput(stdout)
						.redirectError(stderr);					
					
					if (job.getEnv() != null) {
						Map<String, String> env = pb.environment();
						env.clear();
						env.putAll(job.getEnv());
					}
					
					proc = pb.start();
					retcode = proc.waitFor();
					
				} catch (IOException | InterruptedException e) {
					retcode = 100;
				}
				
				queue.jobDone(job.getJobId(), retcode);
				script.delete();
			}
		};
		runningThread.start();
		
	}
	
	public void kill() {
		proc.destroy();
		runningThread.interrupt();
	}

}
