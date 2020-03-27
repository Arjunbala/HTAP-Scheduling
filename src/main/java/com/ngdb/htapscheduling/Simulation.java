package com.ngdb.htapscheduling;

import java.util.List;

import com.ngdb.htapscheduling.cluster.Cluster;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.events.EventQueue;
import com.ngdb.htapscheduling.scheduling.GlobalScheduler;
import com.ngdb.htapscheduling.workload.Workload;
import com.ngdb.htapscheduling.workload.WorkloadFactory;

public class Simulation {

	private static Simulation sInstance = null; // Holder of singleton instance
												// of simulation
	private Cluster cluster;
	private Double mTimeMs; // Current simulation time

	public Simulation() {
		mTimeMs = 0.0;
		cluster = new Cluster(32, 1, 16*1024*1024*1.0); // TODO: Configurable
		Workload w = WorkloadFactory.getInstance().getWorkloadGenerator("test"); // TODO: Configurable
		List<Tuple> tuples = w.getTupleList();
		// Bootstrap the CPU initially with all tuples in it's working set
		// TODO: Think -- should the GPUs initially be empty or pre-populated?
		for(Tuple t : tuples) {
			// All tuples initially have version 0
			cluster.addTupleToCPU(t, 0);
		}
		List<Transaction> transactions = w.getTransactionList();
		// Give it to the global scheduler
		GlobalScheduler gs = GlobalScheduler.createInstance(25.0); // TODO: Configurable
		for(Transaction t : transactions) {
			gs.addTransaction(t);
		}
		gs.startExecution();
		// Now epoch events have been created; we can start the event queue
		EventQueue.getInstance().start();
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
	}

	public static Simulation getInstance() {
		return sInstance;
	}
	
	public Cluster getCluster() {
		return cluster;
	}
}