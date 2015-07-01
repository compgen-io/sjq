package io.compgen.sjq.server;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.Exec;
import io.compgen.cmdline.annotation.Option;
import io.compgen.cmdline.exceptions.CommandArgumentException;
import io.compgen.common.StringUtils;
import io.compgen.sjq.client.ClientException;
import io.compgen.sjq.client.Endpoint;
import io.compgen.sjq.client.SJQClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;

@Command(name="server", category="server", desc="Start an SJQ server")
public class SJQServer {
	public static final String VERSION = "0.9";
	private ThreadedSocketListener threadedSocketListener = null;
	private ThreadedJobQueue threadedJobQueue = null;
	
	private boolean closed = false;

	private int port = 0;
	protected String connFilename = null;
	private String host = "127.0.0.1";
	private String pidfile = null;
	private String passwd = "";
	private String passwdFile = null;
	
	private boolean silent = false;
	private boolean verbose = false;
	
	private String tempDir = null;
	private long timeout_ms = -1;
	
	private int maxProcs = Runtime.getRuntime().availableProcessors();
	private String maxMem = null;
	
	private PrintStream out = System.out;
	private String jobLogFilename = null;

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

	@Option(name="joblog", desc="Write job status log to file")
	public void setJobLogFile(String jobLogFilename) {
			this.jobLogFilename = jobLogFilename;
	}

	@Option(name="log", desc="Write log output to file", charName="l")
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
	@Option(name="conn", desc="Write connection information (ip:port) to a file (default: ~/.sjqserv)", charName="f")
	public void setConnFilename(String connFilename) {
		this.connFilename = connFilename;
	}

	@Option(name="passwd", desc="Set a password for the job-queue (default: none)")
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	@Option(name="passwd-file", desc="Read the password for the job-queue from file (default: ~/.sjqpass)")
	public void setPasswdFile(String filename) throws IOException, CommandArgumentException {
		this.passwdFile = filename;
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
			
			if (jobLogFilename != null) {
				try {
					PrintStream joblog = new PrintStream(new FileOutputStream(jobLogFilename, true));
					joblog.println(threadedJobQueue.getServerStatus());
					joblog.close();
				} catch (FileNotFoundException e) {
					debug("ERROR: Can't write job log! (" + jobLogFilename + ") "+ e.getMessage());
				}
			}
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
		File homedir = new File(System.getProperty("user.home"));
		if (passwd.equals("")) {
			if (passwdFile != null) {
				if (!new File(passwdFile).exists()) {
					throw new CommandArgumentException("Missing password-file: "+passwdFile);
				}
				this.passwd = StringUtils.strip(StringUtils.readFile(passwdFile));
			} else {
				File defaultPasswdFile = new File(homedir, ".sjqpass");
				if (defaultPasswdFile.exists()) {
					this.passwd = StringUtils.strip(StringUtils.readFile(defaultPasswdFile));
				}
			}
		}

		File connFile;
		if (connFilename == null) {
			connFile = new File(homedir, ".sjqserv");
		} else {
			connFile = new File(connFilename);
		}
		
		if (checkConnection(connFile)) {
			log("Connection file: "+connFile.getAbsolutePath()+" exists and is active!");
			System.err.println("Connection file: "+connFile.getAbsolutePath()+" exists and is active!");
			throw new SJQServerException("SJQ server already running!");
		}
		
		if (pidfile != null) {
			StringUtils.writeFile(pidfile, System.getProperty("io.compgen.common.pid"));
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
		threadedSocketListener = new ThreadedSocketListener(this, host, port, connFile, passwd);
		if (!threadedSocketListener.start()) {
			threadedJobQueue.close();
			throw new SJQServerException("Could not start socket listener!");
		}
		
		if (!silent) {
			log("Started Socket Server");
		}
	}
	
	protected boolean checkConnection(File connFile) {
		if (connFile.exists()) {
			try {
				SJQClient client = new SJQClient(Endpoint.readFile(connFile), passwd);
				boolean result = client.ping();
				client.close();
				return result;
			} catch (IOException | ClientException e) {
			}
		} 
		return false;
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
