package io.compgen.sjq.support;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	public interface MapFunc<T, V> {
		public V map(T obj);
	}
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
    public static String join(String delim, String[] args) {
        String out = "";
        
        for (String arg: args) {
            if (out.equals("")) {
                out = arg;
            } else {
                out = out + delim + arg;
            }
        }
        
        return out;
    }

    public static String join(String delim, double[] args) {
        String out = "";
        
        for (Number arg: args) {
            if (out.equals("")) {
                out = ""+arg;
            } else {
                out = out + delim + arg;
            }
        }
        
        return out;
    }

    public static String join(String delim, int[] args) {
        String out = "";
        
        for (Number arg: args) {
            if (out.equals("")) {
                out = ""+arg;
            } else {
                out = out + delim + arg;
            }
        }
        
        return out;
    }
    
    public static String join(String delim, Iterable<? extends Object> args) {
    	return join(delim, args, null);
    }
    public static <T> String join(String delim, Iterable<T> args, MapFunc<T, String> mapper) {
        String out = "";
        if (args != null) {
	        for (T arg: args) {
	        	String val = null;
	        	if (mapper == null) {
	        		val = arg.toString();
	        	} else {
	        		val = mapper.map(arg);
	        	}
	        	
	        	if (val == null) {
	        		continue;
	        	}
	        	
	            if (out.equals("")) {
	                out = val;
	            } else {
	                out = out + delim + val;
	            }
	        }
        }
        
        return out;
    }

	public static String pad(String name, int i, char c) {
		String s = name;
		if (s.length() > i) {
			return s.substring(0, i);
		}
		while (s.length() < c) {
			s += c;
		}
		return s;
	}

	public static void writeFile(String filename, String val) throws IOException {
		writeFile(filename, val, false);
	}
	
	public static void writeFile(String filename, String val, boolean append) throws IOException {
		OutputStream os;
		if (filename.equals("-")) {
			os = System.out;
		} else {
			os = new BufferedOutputStream(new FileOutputStream(filename, append));
		}
		
		os.write(val.getBytes());
		os.flush();

		if (os != System.out) {
			os.close();
		}
		
	}

}
