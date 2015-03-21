package io.compgen.sjq.server;

import io.compgen.annotation.Command;
import io.compgen.annotation.Exec;
import io.compgen.annotation.Option;
import io.compgen.exceptions.CommandArgumentException;
import io.compgen.sjq.support.SJQUtils;

import java.util.Date;

@Command(name="server", category="server", desc="Start up SJQ in server mode")
public class SJQServer {
	private ThreadedSocketListener threadedSocketListener = null;
	private ThreadedJobQueue threadedJobQueue = null;
	
	private boolean closed = false;

	private int port = 0;
	private String portFilename = null;
	private String host = "127.0.0.1";
	
	private boolean silent = false;
	private boolean verbose = false;
	
	private String tempDir = null;
	private long timeout_ms = -1;
	
	private int maxProcs = Runtime.getRuntime().availableProcessors();
	private String maxMem = null;

	@Option(name="port", desc="Port to listen on (default: dynamic)", charName="p")
	public void setPort(int port) {
		this.port = port;
	}
	
	@Option(name="procs", desc="Maximum processors to use (default: all procs/cores)", charName="c")
	public void setMaxProcs(int maxProcs) {
		this.maxProcs = maxProcs;
	}
	
	@Option(name="host", desc="Hostname/IP address to listen on (default: 127.0.0.1)", charName="h")
	public void setHost(String host) {
		this.host = host;
	}

	@Option(name="temp", desc="Temporary directory", charName="T")
	public void setTempDir(String tempDir) {
		this.tempDir = tempDir;
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
	}
	
	public String getSocketAddr() {
		return threadedSocketListener.getSocketAddr();
	}

	@Exec
	public void start() throws CommandArgumentException, SJQServerException {
		log("Max procs: "+maxProcs);
		long maxMemVal = -1;
		if (maxMem!=null) {
			log("Max memory: "+maxMem);
			maxMemVal = SJQUtils.memStrToLong(maxMem);
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
		threadedSocketListener = new ThreadedSocketListener(this, host, port, portFilename);
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
			System.out.println("[" + new Date() + "] " + msg);
		}
	}
	public void debug(String msg) {
		if (verbose) {
			System.out.println("[" + new Date() + "] " + msg);
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

	public boolean isVerbose() {
		return verbose;
	}
	
}
