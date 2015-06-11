package io.compgen.sjq.client;

import io.compgen.common.StringUtils;

import java.io.File;
import java.io.IOException;

public class Endpoint {
	public final String host;
	public final int port;
	
	public Endpoint(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public static Endpoint readFile(File connFile) throws IOException {
		if (connFile == null || !connFile.exists()) {
			throw new IOException("Missing connection file: "+ connFile);
		}
		
		String conn = StringUtils.strip(StringUtils.readFile(connFile));
		String[] hostport = conn.split(":",2);
		
		return new Endpoint(hostport[0], Integer.parseInt(hostport[1]));
	}
	
}
