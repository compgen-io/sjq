package io.compgen.sjq.server;

public enum JobState {
	UNKNOWN("?"),
	HOLD("H"),
	QUEUED("Q"),
	RUNNING("R"),
	SUCCESS("S"),
	ERROR("E"),
	KILLED("K");
	
	private String code;
	
	JobState(String code) {
		this.code = code;
	}
	public String getCode() {
		return this.code;
	}
}
