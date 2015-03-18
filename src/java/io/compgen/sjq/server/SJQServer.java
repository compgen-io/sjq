package io.compgen.sjq.server;

import io.compgen.Exec;
import io.compgen.annotation.Command;
import io.compgen.annotation.Option;
import io.compgen.exceptions.CommandArgumentException;
import io.compgen.sjq.support.SJQUtils;

@Command(name="server", category="server")
public class SJQServer implements Exec {
	private ThreadedSocketListener threadedSocketListener = null;
	private ThreadedJobQueue threadedJobQueue = null;
	private boolean closed = false;

	private int port = 0;
	private String portFilename = null;
	private String host = null;
	
	private String tempDir = null;
	private long timeout_ms = -1;
	
	private int maxProcs = Runtime.getRuntime().availableProcessors();
	private long maxMem = -1;

	@Option(name="port", desc="Port to listen on (default: dynamic)", charName="p")
	public void setPort(int port) {
		this.port = port;
	}
	
	@Option(name="host", desc="Hostname/IP address to listen on (default: dynamic)", charName="h")
	public void setHost(String host) {
		this.host = host;
	}

	@Option(name="temp", desc="Temporary directory", charName="T")
	public void setTempDir(String tempDir) {
		this.tempDir = tempDir;
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
	public void setMaxMemory(String maxMemoryStr) throws CommandArgumentException {
		this.maxMem = SJQUtils.memStrToLong(maxMemoryStr);
		if (this.maxMem == -1) {
			throw new CommandArgumentException("Invalid memory setting: "+maxMemoryStr);
		}
	}
	
	public void shutdown() {
		if (!closed) {
			closed = true;
			System.out.println("Shutting down server.");
			
			threadedSocketListener.close();
			threadedJobQueue.close();
		}
	}
	
	public String getSocketAddr() {
		return threadedSocketListener.getSocketAddr();
	}

	@Override
	public void exec() throws Exception {
		threadedJobQueue = new ThreadedJobQueue(this, maxProcs, maxMem, timeout_ms, tempDir);
		if (threadedJobQueue.start()) {
			System.err.println("Started Job Queue.");
			threadedSocketListener = new ThreadedSocketListener(this, host, port, portFilename);
			if (threadedSocketListener.start()) {
				System.err.println("Started Socket Server.");
			} else {
				threadedJobQueue.close();
			}
		}
	}
	
	public ThreadedJobQueue getQueue() {
		return threadedJobQueue;
	}
	
}
