package io.compgen.sjq;

import java.io.IOException;
import java.io.InputStream;

public class SJQ {

	public static void main(String[] args) {
		usage();
	}

	private static void showFile(String fname) throws IOException {
		InputStream is = SJQ.class.getClassLoader().getResourceAsStream(fname);
		if (is == null) {
			throw new IOException("Can't load file: "+fname);
		}
		int c;
		while ((c = is.read()) > -1) {
			System.out.print((char) c);
		}
		is.close();	
	}
	
	private static void usage() {
		try {
			showFile("io/compgen/sjq/USAGE.txt");
			System.out.println();
			System.out.println("http://compgen.io/sjq");
			showFile("VERSION");
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void license() {
		try {
			showFile("LICENSE");
			showFile("INCLUDES");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
