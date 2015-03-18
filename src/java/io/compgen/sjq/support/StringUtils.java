package io.compgen.sjq.support;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	public static String strip(String str) {
		return lstrip(rstrip(str));
	}
	
    public static String rstrip(String str) {
        Pattern pattern = Pattern.compile("^(.*?)\\s*$");        
        Matcher m = pattern.matcher(str);
        if (m.find()) {        
            return m.group(1);
        }
        return str;
    }
    
    public static String lstrip(String str) {
        Pattern pattern = Pattern.compile("^\\s*(.*?)$");
        Matcher m = pattern.matcher(str);
        if (m.find()) {        
            return m.group(1);
        }
        return str;
    }
    

	public static String readFile(String filename) throws IOException {
		String s = "";
		InputStream is;
		if (filename.equals("-")) {
			is = System.in;
		} else {
			is = new FileInputStream(filename);
		}
		byte[] buf = new byte[4096];
		int read = 0;
		while ((read = is.read(buf, 0, buf.length)) > -1) {
			s += new String(buf,0,read);
		}
		if (is != System.in) {
			is.close();
		}
		
		return s;
	}
	
    public static List<String> quotedSplit(String str, String delim) {
    	return quotedSplit(str, delim, false);
    }
    public static List<String> quotedSplit(String str, String delim, boolean includeDelim) {
		List<String> tokens = new ArrayList<String>();

		String buf="";
		boolean inquote = false;
		int i=0;
		
		while (i < str.length()) {			
			if (inquote) {
				if (str.charAt(i) == '"') {
					if (buf.endsWith("\\")) {
						buf = buf.substring(0, buf.length()-1) + "\"";
					} else {
						buf += "\"";
						inquote = false;
					}
				} else {
					buf += str.charAt(i);
				}
				i++;
				continue;
			}

			if (str.charAt(i) == '"') {
				buf += "\"";
				inquote = true;
				i++;
				continue;
			}

			if (str.substring(i, i+delim.length()).equals(delim)) {
				if (buf.length()>0) {
					tokens.add(buf);
					if (includeDelim) {
						tokens.add(delim);
					}
					buf = "";
				}
				i += delim.length();
				continue;
			}
			
			buf += str.charAt(i);
			i++;
		}
		
		if (!buf.equals("")) {
			tokens.add(buf);
		}
		
		return tokens;
    }

}
