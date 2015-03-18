package io.compgen.sjq.support;

import java.util.ArrayList;
import java.util.List;

public class ListUtils {

	public static <T> List<T> build(T[] elements) {
		List<T> l = new ArrayList<T>();
		for (T el: elements) {
			l.add(el);
		}
		return l;
	}

}
