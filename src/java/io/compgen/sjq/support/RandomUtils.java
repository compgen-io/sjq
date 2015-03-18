package io.compgen.sjq.support;

import java.util.Random;

public class RandomUtils {
	final private static String sym = "abcdefghijklmnopqrstuvwxyz0123456789";
	final private static Random rand = new Random();
	
	public static String randomString(int len) {
		String s="";
		while (s.length()<len) {
			s += sym.charAt(rand.nextInt(sym.length()));
		}
		return s;
	}
}
