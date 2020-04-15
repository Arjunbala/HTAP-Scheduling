package com.ngdb.htapscheduling;

public class Logging {
	public static Logging sInstance = null;
	private String mLogLevel; // "metrics", "info", "debug"
	public static String INFO = "info";
	public static String METRICS = "metrics";
	public static String DEBUG = "debug";

	public static Logging getInstance() {
		if (sInstance == null) {
			sInstance = new Logging();
		}
		return sInstance;
	}

	private Logging() {
		mLogLevel = INFO;
	}

	public void log(String msg, String level) {
		if (mLogLevel.equals(DEBUG)) {
			System.out.println(Simulation.getInstance().getTime() + " " + level
					+ ": " + msg);
		} else if (mLogLevel.equals(INFO)) {
			if (!level.equals(DEBUG)) {
				System.out.println(Simulation.getInstance().getTime() + " "
						+ level + ": " + msg);
			}
		} else {
			if (level.equals(METRICS)) {
				System.out.println(Simulation.getInstance().getTime() + " "
						+ level + ": " + msg);
			}
		}
	}

}