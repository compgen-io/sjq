package io.compgen.sjq.server;

import io.compgen.common.MonitoredThread;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class ThreadedSocketListener {
	protected int port = 0;
	protected File connFile = null;
	protected String listenIP = null;
	protected String passwd = null;
	
	protected ServerSocket socket = null;
	private boolean closed = false;
	
	private List<SessionHandler> sessions = new ArrayList<SessionHandler>();
	final private SJQServer server;
	
	private MonitoredThread thread = null;
	
	public ThreadedSocketListener(SJQServer server, String listenIP, int port, File connFile, String passwd) {
		this.server = server;
		this.listenIP = listenIP;
		this.port = port;
		this.connFile = connFile;
		this.passwd = passwd;
	}
	
	public void close() {
		if (!closed) {
			closed = true;

			if (connFile != null) {
				connFile.delete();
			}
			
			for (SessionHandler sh: sessions) {
				try {
					sh.close();
				} catch (IOException e) {
				}
			}
			try {
				socket.close();
			} catch (IOException e) {
			}
			server.log("Shutdown socket listener...");
		}
	}
	
	public boolean start() {
		try {
			if (listenIP != null) {
				socket = new ServerSocket();
				socket.bind(new InetSocketAddress(listenIP, port));
			} else {
				socket = new ServerSocket(port);
			}
			socket.setReuseAddress(true);
			closed = false;
	
			if (connFile != null) {
				if (connFile.exists()) {
					connFile.delete();
				}
				connFile.createNewFile();
				connFile.setReadable(false,  false);
				connFile.setWritable(false,  false);
				connFile.setExecutable(false,  false);
				connFile.setReadable(true,  true);
				connFile.setWritable(true,  true);

				OutputStream os = new FileOutputStream(connFile);
				os.write((getSocketAddr()+"\n").getBytes());
				os.close();

				connFile.deleteOnExit();
			}
		} catch (IOException e) {
			server.log("Error starting socket listener! " + e.getMessage());
			return false;
		}

		server.log("Listening for clients on: "+getSocketAddr());

		thread = new MonitoredThread(new Runnable() {
			public void run() {
				while (!closed) {
					try{
						while (!closed) {
							Socket client = socket.accept(); 
							server.log("New client connection: "+ client.getInetAddress().getHostAddress()+":"+client.getPort());
							SessionHandler sh = new SessionHandler(client, server, passwd);
							Thread t = new Thread(sh);
							sessions.add(sh);
							t.start();
						}
					} catch (SocketException e) {
						break;
					} catch (IOException e) {
						break;
					}
				}
			}
		});
		thread.start();
		return true;
	}

//	public void setListenHost(String host) {
//		this.listenIP = host;
//	}
//
//	public void setConnFile(String connFile) {
//		this.connFile = portFilename;
//	}
//
	public String getSocketAddr() {
		if (socket != null) {
			return socket.getInetAddress().getHostAddress()+":"+socket.getLocalPort();
		} 
		return null;
	}
	public void join() {
		while (this.thread != null && !this.thread.isDone()) {
			try {
				this.thread.join(1000);
			} catch (InterruptedException e) {
			}
		}
	}
}
