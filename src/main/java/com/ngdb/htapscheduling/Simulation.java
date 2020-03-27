package com.ngdb.htapscheduling;

import com.ngdb.htapscheduling.cluster.Cluster;

public class Simulation {

	private static Simulation sInstance = null; // Holder of singleton instance
												// of simulation
	private Cluster cluster;
	private Double mTimeMs; // Current simulation time

	public Simulation() {
		mTimeMs = 0.0;
		cluster = new Cluster(32, 1, 16*1024*1024*1.0);
	}

	/**
	 * Return current simulation time
	 * 
	 * @return Double, indicating the time
	 */
	public Double getTime() {
		return mTimeMs;
	}

	/**
	 * Set current simulation time
	 * 
	 * @param Double, indicating time
	 */
	public void setTime(Double time) {
		mTimeMs = time;
	}

	public static void main(String args[]) {
		System.out.println("Hello world");
		Simulation s = new Simulation();
		// TODO: Bootstrap table creations, transaction executions
	}

	public static Simulation getInstance() {
		return sInstance;
	}
	
	public Cluster getCluster() {
		return cluster;
	}
}