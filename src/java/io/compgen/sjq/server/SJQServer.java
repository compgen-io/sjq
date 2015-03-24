package io.compgen.sjq.server;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.Exec;
import io.compgen.cmdline.annotation.Option;
import io.compgen.cmdline.exceptions.CommandArgumentException;
import io.compgen.support.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;

@Command(name="server", category="server", desc="Start up SJQ in server mode")
public class SJQServer {
	private ThreadedSocketListener threadedSocketListener = null;
	private ThreadedJobQueue threadedJobQueue = null;
	
	private boolean closed = false;

	private int port = 0;
	private String portFilename = null;
	private String host = "127.0.0.1";
	private String pidfile = null;
	private String passwd = "";
	
	private boolean silent = false;
	private boolean verbose = false;
	
	private String tempDir = null;
	private long timeout_ms = -1;
	
	private int maxProcs = Runtime.getRuntime().availableProcessors();
	private String maxMem = null;
	
	private PrintStream out = System.out;

	@Option(name="port", desc="Port to listen on (default: dynamic)", charName="p")
	public void setPort(int port) {
		this.port = port;
	}
	
	@Option(name="procs", desc="Maximum processors to use (default: all procs/cores)", charName="c")
	public void setMaxProcs(int maxProcs) {
		this.maxProcs = maxProcs;
	}
	
	@Option(name="pid", desc="Write the process-id to a file")
	public void setPIDFile(String pidfile) {
		this.pidfile = pidfile;
	}

	@Option(name="log", desc="Writer output to log to file", charName="l")
	public void setLogFile(String logfile) throws FileNotFoundException {
		if (logfile != null) {
			out = new PrintStream(new FileOutputStream(logfile, true));
		}
	}

	@Option(name="host", desc="Hostname/IP address to listen on (default: 127.0.0.1)", charName="h")
	public void setHost(String host) {
		this.host = host;
	}

	@Option(name="temp", desc="Temporary directory", charName="T")
	public void setTempDir(String tempDir) {
		this.tempDir = tempDir;
	}
	@Option(name="passwd", desc="Set a password for the job-queue")
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	@Option(name="passwd-file", desc="Read the password for the job-queue from file")
	public void setPasswdFile(String filename) throws IOException, CommandArgumentException {
		if (!new File(filename).exists()) {
			throw new CommandArgumentException("Missing password-file: "+filename);
		}
		this.passwd = StringUtils.strip(StringUtils.readFile(filename));
	}

	@Option(desc="Verbose logging", charName="v")
	public void setVerbose(boolean val) {
		this.verbose = val;
	}
	
	@Option(name="silent", desc="Suppress log messages", charName="s")
	public void setSilent(boolean val) {
		this.silent = val;
	}

	@Option(name="timeout_ms", desc="Shutdown the server if idle for N sec", charName="t")
	public void setTimeout(int timeout) {
		this.timeout_ms = timeout * 1000;
	}

	@Option(name="file", desc="Write the listening ip:port to a file", charName="f")
	public void setPortFilename(String portFilename) {
		this.portFilename = portFilename;
	}

	@Option(name="mem", desc="Maximum memory to use for running jobs")
	public void setMaxMemory(String maxMem) throws CommandArgumentException {
		this.maxMem = maxMem;
	}
	
	public void shutdown() {
		if (!closed) {
			closed = true;
			log("Shutting down server");
			
			threadedJobQueue.close();
			threadedSocketListener.close();
		}

		if (out != System.out) {
			out.close();
		}
	}
	
	public String getSocketAddr() {
		return threadedSocketListener.getSocketAddr();
	}

	@Exec
	public void start() throws CommandArgumentException, SJQServerException, IOException {
		if (pidfile != null) {
			StringUtils.writeFile(pidfile, System.getProperty("io.compgen.support.pid"));
			new File(pidfile).deleteOnExit();
		}
				
		log("Max procs: "+maxProcs);
		long maxMemVal = -1;
		if (maxMem!=null) {
			log("Max memory: "+maxMem);
			maxMemVal = ThreadedJobQueue.memStrToLong(maxMem);
			if (maxMemVal == -1) {
				throw new CommandArgumentException("Invalid memory setting: "+maxMem);
			}
		}
		threadedJobQueue = new ThreadedJobQueue(this, maxProcs, maxMemVal, timeout_ms, tempDir);

		if (!threadedJobQueue.start()) {
			throw new SJQServerException("Could not start job queue!");
		}
		
		if (!silent) {
			log("Started Job Queue");
		}
		threadedSocketListener = new ThreadedSocketListener(this, host, port, portFilename, passwd);
		if (!threadedSocketListener.start()) {
			threadedJobQueue.close();
			throw new SJQServerException("Could not start socket listener!");
		}
		
		if (!silent) {
			log("Started Socket Server");
		}
	}
	
	public ThreadedJobQueue getQueue() {
		return threadedJobQueue;
	}
	
	public boolean isSilent() {
		return silent;
	}
	
	public void log(String msg) {
		if (!silent) {
			out.println("[" + new Date() + "] " + msg);
		}
	}
	public void debug(String msg) {
		if (verbose) {
			out.println("[" + new Date() + "] " + msg);
		}
	}
	
	public void join() {
		if (threadedJobQueue != null) {
			threadedJobQueue.join();
		}
		if (threadedSocketListener != null) {
			threadedSocketListener.join();
		}
	}
}
