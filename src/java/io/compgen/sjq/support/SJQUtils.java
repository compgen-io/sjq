package io.compgen.sjq.support;

public class SJQUtils {
	public static long memStrToLong(String memVal) {
		if (memVal.toUpperCase().endsWith("G")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024 * 1024 * 1024;
		} else if (memVal.toUpperCase().endsWith("M")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024 * 1024;
		} else if (memVal.toUpperCase().endsWith("K")) {
			return Long.parseLong(memVal.substring(0, memVal.length()-1)) * 1024;
		}
		try {
			return Long.parseLong(memVal);
		} catch (NumberFormatException e) {
			return -1;
		}
	}
}
