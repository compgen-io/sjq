package io.compgen.sjq.client;

import java.io.IOException;

public class ClientException extends Exception {

	public ClientException(IOException e) {
		super(e);
	}
	public ClientException(String e) {
		super(e);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
