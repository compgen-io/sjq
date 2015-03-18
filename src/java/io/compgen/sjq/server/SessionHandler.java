package io.compgen.sjq.server;

import io.compgen.sjq.support.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class SessionHandler implements Runnable {
	private Socket client;
	private SJQServer server;
	private boolean closed = false;
	
	public SessionHandler(Socket client, SJQServer server) {
		this.client = client;
		this.server = server;
	}

	public void run() {
		while (!closed) {
			try {
				String s = readLine();
				if (s == null || s.equals("")) {
					break;
				}
				
				String[] line = s.split(" ", 2);
				String cmd = line[0].toUpperCase();

				switch(cmd){

				case "QUIT":
//					System.err.println("Closing client connection.");
					writeLine("OK BYE!");
					close();
					break;
				case "SHUTDOWN":
					// Note: there is no security on this at all...
					writeLine("OK SHUTDOWN!");
					close();
					server.shutdown();
					break;
				case "HELP":
					writeLine("OK Valid Commands: QUIT PING STATUS SUBMIT HELP");
					break;
				case "STATUS":
					{
						if (line.length == 2 && line[1] != null && !line[1].equals("")) {
							String jobId = line[1];
//							System.err.println("Looking for job: "+jobId);
							Job job = server.getQueue().getJob(jobId);
							if (job != null) {
								writeLine("OK "+jobId+" "+job.getState().getCode());
							} else {
								writeLine("ERROR "+jobId+" not found!");
							}
						} else {
							writeLine(server.getQueue().getStatus());
						}
					}
					break;
				case "DETAIL":
					{
						String jobId = line[1];
						Job job = server.getQueue().getJob(jobId);
						if (job != null) {
							writeLine("OK "+jobId+" "+job.getState().getCode());
							writeLine("PROCS " + job.getProcs());
							if (job.getMem() > 0) {
								writeLine("MEM " + job.getMem());
							}
							
							writeLine("CWD " + job.getCwdDefault());
							if (job.getEnv() != null) {
								for (String k: job.getEnv().keySet()) {
									writeLine("ENV " + k +"="+job.getEnv().get(k));
								}
							}
							if (job.getStdout()!=null) {
								writeLine("STDOUT " + job.getStdout());
							}
							if (job.getStderr()!=null) {
								writeLine("STDERR " + job.getStderr());
							}
							writeLine("SUBMIT " + job.getSubmitTime());
							writeLine("START " + job.getStartTime());
							writeLine("END " + job.getEndTime());
							writeLine("RETCODE " + job.getRetCode());
							writeLine("OK");
						} else {
							writeLine("ERROR "+jobId+" not found!");
						}
					}
					break;
				case "SUBMIT":
					buildJob(line[1]);
					break;
				case "PING":
					writeLine("OK PONG");
					break;
				default:
					writeLine("ERROR " + s);
				
				}
			} catch (IOException e) {
				break;
			}
		}
		try {
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void buildJob(String name) throws IOException {
		Job job = new Job(name);
		
		String body = null;
		while (!closed && body == null) {
			String s = readLine();
			if (s == null || s.equals("")) {
				break;
			}
			
			String[] line = s.split(" ", 2);
			String cmd = line[0].toUpperCase();
			
			switch(cmd) {
			case "CWD":
				job.setCwd(line[1]);
				break;
			case "STDOUT":
				job.setStdout(line[1]);
				break;
			case "STDERR":
				job.setStderr(line[1]);
				break;
			case "ENV":
				List<String> env = StringUtils.quotedSplit(line[1], "=");
				if (env.size() == 2) {
					job.addEnv(env.get(0), env.get(1));
				}
				break;
			case "PROCS":
				job.setProcs(Integer.parseInt(line[1]));
				break;
			case "MEM":
				job.setMem(line[1]);
				break;
			case "DEP":
				job.addWaitForJob(line[1]);
				break;
			case "BODY":
				int size = Integer.parseInt(line[1]);
				body = readBytes(size);
				job.setBody(body);
				break;
			}
		}

		
		String jobId = this.server.getQueue().addJob(job);
		writeLine("OK "+jobId);
	}

	public void close() throws IOException {
		if (!closed) {
			this.closed = true;
			client.close();
		}
	}

	protected void writeLine(String s) throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		OutputStream os = this.client.getOutputStream();
		os.write((s+"\r\n").getBytes());
		os.flush();
//		System.err.println(">>> " + s);
	}

	protected String readLine() throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		String s = "";
		InputStream is = this.client.getInputStream();
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
//		System.err.println("<<< " + s);
		return s;
	}

	protected String readBytes(int bytes) throws IOException {
		if (closed) {
			throw new IOException("Socket closed");
		}
		byte[] buf = new byte[bytes];
		InputStream is = this.client.getInputStream();
		int read = 0;

		while (read < bytes) {
			int s = is.read(buf, read, bytes-read);

			if (s == -1) {
				break;
			} else {
				read += s;
			}
		}
//		System.err.println("<<< <" + bytes+" bytes>");
		return new String(buf, 0, bytes);
	}

}
