package io.compgen.sjq.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
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
		final File monitorFile;
		try {
			script = File.createTempFile("sjq-"+job.getJobId()+"-", ".sh", queue.getTempDir());
			script.createNewFile();
			script.setExecutable(false, false);
			script.setReadable(false, false);
			script.setWritable(false, false);
			script.setExecutable(true, true);
			script.setReadable(true, true);
			script.setWritable(true, true);
			script.deleteOnExit();

			OutputStream os = new FileOutputStream(script);
			os.write(job.getBody().getBytes());
			os.close();

			monitorFile = File.createTempFile("sjq-"+job.getJobId()+"-", ".monitor.sh", queue.getTempDir());
			monitorFile.createNewFile();
			monitorFile.setExecutable(false, false);
			monitorFile.setReadable(false, false);
			monitorFile.setWritable(false, false);
			monitorFile.setExecutable(true, true);
			monitorFile.setReadable(true, true);
			monitorFile.setWritable(true, true);
			monitorFile.deleteOnExit();

			// TODO: chown file? sticky bit?
//			UserPrincipalLookupService lookupService = FileSystems.getDefault().getUserPrincipalLookupService();
//			UserPrincipal user = lookupService.lookupPrincipalByName("foo");
//			Files.setOwner(monitorFile.toPath(), user);
//			PosixFilePermissions perm = new PosixFilePermissions(); 
//			Files.getFileAttributeView(monitorFile.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setPermissions(perms);
			
			String monitorScript = "#!/bin/sh\n"+
					"trap 'pkill -TERM -P $PID; exit 127' INT TERM EXIT\n" +
					script.getAbsolutePath() + " & \n" +
					"PID=$!\n" +
					"wait $PID\n"+
					"RETVAL=$?\n"+
					"trap - INT TERM EXIT\n" +
					"wait\nexit $RETVAL\n";
					
			OutputStream monOS = new FileOutputStream(monitorFile);
			monOS.write(monitorScript.getBytes());
			monOS.close();

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
					stdout = new File(new File(job.getCwdDefault()), job.getName()+"."+job.getJobId()+".stdout");
				}
				
				File stderr;
				if (job.getStderr() != null) { 
					stderr = new File(job.getStderr());
				
					if (stderr.isDirectory()) {
						stderr = new File(stderr, job.getName()+"."+job.getJobId()+".stderr");
					}
				} else {
					stderr = new File(new File(job.getCwdDefault()), job.getName()+"."+job.getJobId()+".stderr");
				}
				
				try {
					ProcessBuilder pb = new ProcessBuilder()
						.command(monitorFile.getAbsolutePath())
						.directory(new File(job.getCwdDefault()))
						.redirectOutput(stdout)
						.redirectError(stderr);					
					
					Map<String, String> env = pb.environment();
					if (job.getEnv() != null) {
						env.clear();
						env.putAll(job.getEnv());
					}
					env.put("JOB_ID", job.getJobId());
					
					proc = pb.start();
					retcode = proc.waitFor();
					proc.waitFor();
					
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
					retcode = 100;
				}
				
				queue.jobDone(job.getJobId(), retcode);
				script.delete();
				monitorFile.delete();
			}
		};
		runningThread.start();
		
	}
	
	public void kill() {
		queue.getServer().log("Process kill - " + job.getJobId() + " start");
		
		// Note: This may not work correctly - Java won't kill any subprocesses.
		proc.destroy();

		try {
 			runningThread.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		queue.getServer().log("Process kill - " + job.getJobId() + " done");
	}

}
