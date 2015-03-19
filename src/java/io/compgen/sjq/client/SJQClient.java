package io.compgen.sjq.client;

import io.compgen.sjq.server.Job;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

public class SJQClient {
	final private Socket socket;
	private boolean closed = false;
	
	public SJQClient(String host, int port) throws UnknownHostException, IOException {
		this.socket = new Socket(host, port);
	}
	
	protected void writeLine(String s) throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		OutputStream os = this.socket.getOutputStream();
		os.write((s+"\r\n").getBytes());
		os.flush();
	}

	protected void writeBytes(byte[] bytes) throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		OutputStream os = this.socket.getOutputStream();
		os.write(bytes);
		os.flush();
	}

	protected String readLine() throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		String s = "";
		InputStream is = this.socket.getInputStream();
		int b;
		while ((b = is.read())>-1) {
			s += (char) b;
			if (s.endsWith("\n")) {
				break;
			}
		}

		if (s.endsWith("\r\n")) {
			s = s.substring(0, s.length()-2);
		} else if (s.endsWith("\n")) {
			s = s.substring(0, s.length()-1);
		}
		return s;
	}

	protected String readBytes(int bytes) throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		byte[] buf = new byte[bytes];
		InputStream is = this.socket.getInputStream();
		int read = 0;

		while (read < bytes) {
			int s = is.read(buf, read, bytes-read);

			if (s == -1) {
				break;
			} else {
				read += s;
			}
		}
		return new String(buf, 0, bytes);
	}

	public boolean ping() throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		try {
			writeLine("PING");
			String result = readLine();
			if (result.equals("OK PONG")) {
				return true;
			}
			throw new ClientException(result);
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}

	public void shutdown() throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		
		try {
			writeLine("SHUTDOWN");
			readLine();
			this.socket.close();
			closed = true;
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}

	public void close() throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		
		try {
			writeLine("QUIT");
			readLine();
			this.socket.close();
			closed = true;
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}

	public String getStatus(String jobId) throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		try {
			if (jobId == null) {
				writeLine("STATUS");
			} else {
				writeLine("STATUS "+jobId);
			}
			String result = readLine();
			if (!result.startsWith("OK ")) {
				throw new ClientException(result);
			}
			return result.substring(3);
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}
	public String submitJob(String name, String body, int procs, String mem, String stderr, String stdout, String cwd, Map<String, String> env, Iterable<String> deps) throws ClientException {
	
		Job job = new Job(name);
		job.setProcs(procs);
		if (mem != null) {
			job.setMem(mem);
		}
		if (stderr != null) {
			job.setStderr(stderr);
		}
		if (stdout != null) {
			job.setStdout(stdout);
		}
		if (cwd != null) {
			job.setCwd(cwd);
		}
		if (env != null) {
			job.setEnv(env);
		}
		if (deps != null) {
			for (String dep: deps){
				if (dep != null && !dep.equals("")) {
					job.addWaitForJob(dep);
				}
			}
		}
		job.setBody(body);
		
		return submitJob(job);
	}

	public String submitJob(Job job) throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		try {
			writeLine("SUBMIT " + job.getName());
			writeLine("PROCS " + job.getProcs());
			if (job.getMem() > 0) {
				writeLine("MEM " + job.getMem());
			}
			
			if (job.getCwd() != null) {
				writeLine("CWD " + job.getCwd());
			}

			if (job.getStderr() != null) {
				writeLine("STDERR " + job.getStderr());
			}

			if (job.getStdout() != null) {
				writeLine("STDOUT " + job.getStdout());
			}

			if (job.getEnv() != null) {
				for (String k: job.getEnv().keySet()) {
					writeLine("ENV \"" + k + "\"=\"" + job.getEnv().get(k)+"\"");
				}
			}

			if (job.getWaitFor() != null) {
				for (String dep: job.getWaitFor()) {
					if (dep != null && !dep.equals("")) {
						writeLine("DEP " + dep);
					}
				}
			}

			writeLine("BODY " + job.getBody().length());
			writeBytes(job.getBody().getBytes());
			
			String result = readLine();
			if (!result.startsWith("OK ")) {
				throw new ClientException(result);
			}
			return result.substring(3);
			
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}

	public void getDetailedStatus(String jobId, PrintStream out) throws ClientException {
		if (closed) {
			throw new ClientException("Closed connection");
		}
		try {
			writeLine("DETAIL "+jobId);
			String result = readLine();
			while (!result.equals("OK") && !result.equals("")) {
				out.println(result);
				result = readLine();
			}
		} catch (IOException e) {
			throw new ClientException(e);
		}
		
	}
}
