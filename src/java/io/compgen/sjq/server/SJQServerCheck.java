package io.compgen.sjq.server;

import io.compgen.cmdline.annotation.Command;
import io.compgen.cmdline.annotation.Exec;
import io.compgen.cmdline.exceptions.CommandArgumentException;

import java.io.File;
import java.io.IOException;

@Command(name="server-check", desc="Check to see if a server will start...", hidden=true)

public class SJQServerCheck extends SJQServer {
	@Exec
	public void start() throws CommandArgumentException, SJQServerException, IOException {
		File homedir = new File(System.getProperty("user.home"));

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
	}
}
