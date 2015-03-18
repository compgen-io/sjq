package io.compgen.sjq.support;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Counter<T> {
	private Map<T, Integer> counts;
	
	public Counter() {
		this(new HashMap<T, Integer>());
	}
	public Counter(Map<T, Integer> impl) {
		this.counts = new HashMap<T, Integer>();
	}
	
	public void incr(T key) {
		if (counts.containsKey(key)) {
			counts.put(key, counts.get(key) + 1);
		} else {
			counts.put(key, 1);
		}
	}

	public Map<T, Integer> getCounts() {
		return counts;
	}
	public Set<T> keySet() {
		return counts.keySet();
	}

	public Integer get(T key) {
		return counts.get(key);
	}
}
