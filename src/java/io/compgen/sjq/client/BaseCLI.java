package io.compgen.sjq.client;

import io.compgen.cmdline.annotation.Exec;
import io.compgen.cmdline.annotation.Option;
import io.compgen.cmdline.exceptions.CommandArgumentException;
import io.compgen.common.StringUtils;

import java.io.File;
import java.io.IOException;

public abstract class BaseCLI {
	private String connFilename = null;
	private int port = -1;
	private String host = null;
	private String passwd = "";
	private String passwdFilename = null;
	
	@Option(name="port", desc="Port server is listening on", charName="p")
	public void setPort(int port) {
		this.port = port;
	}
	
	@Option(name="host", desc="Hostname/IP address to connect to (default: 127.0.0.1)", charName="h")
	public void setHost(String host) {
		this.host = host;
	}


	@Option(name="conn", desc="Read connection information (ip:port) from a file (default: ~/.sjqserv)", charName="f")
	public void setConnFilename(String connFilename) {
		this.connFilename = connFilename;
	}

	@Option(name="passwd", desc="Set a password for the job-queue (default: none)")
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	@Option(name="passwd-file", desc="Read the password for the job-queue from file (default: ~/.sjqpass)")
	public void setPasswdFile(String filename) throws IOException, CommandArgumentException {
		this.passwdFilename = filename;
	}

	@Exec
	public void exec()  throws Exception {
		File homedir = new File(System.getProperty("user.home"));
		File connFile;
		if (connFilename == null) {
			connFile = new File(homedir, ".sjqserv");
		} else {
			connFile = new File(connFilename);
		}

		if (host == null && port == -1 && !connFile.exists()) {
			throw new CommandArgumentException("You must specify host and port or a connection file.");
		}

		if (passwd.equals("")) {
			if (passwdFilename != null) {
				if (!new File(passwdFilename).exists()) {
					throw new CommandArgumentException("Missing password-file: "+passwdFilename);
				}
				this.passwd = StringUtils.strip(StringUtils.readFile(passwdFilename));
			} else {
				File defaultPasswdFile = new File(homedir, ".sjqpass");
				if (defaultPasswdFile.exists()) {
					this.passwd = StringUtils.strip(StringUtils.readFile(defaultPasswdFile));
				}
			}
		}

		Endpoint endpoint = new Endpoint(host, port);
		if (connFile != null && connFile.exists()) {
			endpoint = Endpoint.readFile(connFile);
		}
//		
//		System.err.println("Connecting to: " + host + ":" + port);

		SJQClient client = new SJQClient(endpoint, passwd);
		process(client);
	}

	protected abstract void process(SJQClient client) throws Exception;
}
