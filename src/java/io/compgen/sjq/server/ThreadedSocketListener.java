package io.compgen.sjq.server;

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
	protected String socketFilename = null;
	protected String listenIP = null;
	
	protected ServerSocket socket = null;
	private boolean closed = false;
	
	private List<SessionHandler> sessions = new ArrayList<SessionHandler>();
	final private SJQServer server;
		
	public ThreadedSocketListener(SJQServer server, String listenIP, int port, String socketFilename) {
		this.server = server;
		this.listenIP = listenIP;
		this.port = port;
		this.socketFilename = socketFilename;
	}
	
	public void close() {
		if (!closed) {
			closed = true;

			if (socketFilename != null) {
				File f = new File(socketFilename);
				f.delete();
			}
			
			for (SessionHandler sh: sessions) {
				try {
					sh.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				socket.close();
			} catch (IOException e) {
			}
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
	
			if (socketFilename != null) {
				final File f = new File(socketFilename);
				f.createNewFile();
				f.setReadable(false,  false);
				f.setWritable(false,  false);
				f.setExecutable(false,  false);
				f.setReadable(true,  true);
				f.setWritable(true,  true);
				OutputStream os = new FileOutputStream(f);
				os.write((getSocketAddr()+"\n").getBytes());
				os.close();
				
				Runtime.getRuntime().addShutdownHook(new Thread() {
					public void run() {
						if (f.exists()) {
							f.delete();
						}
					}
				});
				
			}
		} catch (IOException e) {
			System.err.println("Error starting socket listener! " + e.getMessage());
			return false;
		}

		System.out.println("Listening for clients on: "+getSocketAddr());

		new Thread() {
			public void run() {
				while (!closed) {
					try{
						while (!closed) {
							Socket client = socket.accept(); 
							SessionHandler sh = new SessionHandler(client, server);
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
		}.start();
		return true;
	}

	public void setListenHost(String host) {
		this.listenIP = host;
	}

	public void setListenFilename(String portFilename) {
		this.socketFilename = portFilename;	
	}

	public String getSocketAddr() {
		if (socket != null) {
			return socket.getInetAddress().getHostAddress()+":"+socket.getLocalPort();
		} 
		return null;
	}
}
