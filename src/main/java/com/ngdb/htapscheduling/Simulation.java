package com.ngdb.htapscheduling;

import org.json.simple.JSONObject;
import java.util.List;

import com.ngdb.htapscheduling.Logging;
import com.ngdb.htapscheduling.cluster.Cluster;
import com.ngdb.htapscheduling.config.ConfigUtils;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.events.EventQueue;
import com.ngdb.htapscheduling.scheduling.GlobalScheduler;
import com.ngdb.htapscheduling.scheduling.TransactionExecutor;
import com.ngdb.htapscheduling.scheduling.TransactionScheduler;
import com.ngdb.htapscheduling.workload.Workload;
import com.ngdb.htapscheduling.workload.WorkloadFactory;

public class Simulation {

	private static Simulation sInstance = null; // Holder of singleton instance
												// of simulation
	private static Cluster cluster;
	private Double mTimeMs; // Current simulation time

	public Double TransactionStartTime; //start time for transactions
	public Double TransactionEndTime; //End time for all transactions

	public Simulation(JSONObject clusterConfig, JSONObject memMgmtConfig) {
		mTimeMs = 0.0;
		cluster = new Cluster(clusterConfig, memMgmtConfig);
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
		// Format: args <cluster> <workload> <scheduler> <mem_mgmt>

		JSONObject clusterConfig = ConfigUtils.getClusterConfig(args[0]);
		JSONObject workloadConfig = ConfigUtils.getWorkloadConfig(args[1]);
		JSONObject schedulerConfig = ConfigUtils.getSchedulerConfig(args[2]);
		JSONObject memMgmtConfig = ConfigUtils.getMemoryMgmtConfig(args[3]);

		sInstance = new Simulation(clusterConfig, memMgmtConfig);
		Workload w = WorkloadFactory.getInstance().getWorkloadGenerator(workloadConfig); 
		List<Tuple> tuples = w.getTupleList();
		// Bootstrap the CPU initially with all tuples in it's working set
		// TODO: Think -- should the GPUs initially be empty or pre-populated?
		long count = 0;
		/*for(Tuple t : tuples) {
			// All tuples initially have version 0
			if(count % 100000 == 0) {
				System.out.println(t.getTableName() + t.getId());
			}
			count++;
			cluster.addTuplesToCPU(t, 0);
		}*/
		cluster.addTuplesToCPU(tuples, 0);
		//cluster.printCPUWorkingSet();
		List<Transaction> transactions = w.getTransactionList();
		// Give it to the global scheduler
		GlobalScheduler gs = GlobalScheduler.createInstance(schedulerConfig); 
		TransactionScheduler ts = TransactionScheduler.createInstance(schedulerConfig); 

		for(Transaction t : transactions) {
			gs.addTransaction(t);
		}
		gs.printTransactionList();
		gs.startExecution();
		sInstance.TransactionStartTime = sInstance.getTime();
		// Now epoch events have been created; we can start the event queue
		EventQueue.getInstance().start();

		sInstance.TransactionEndTime = sInstance.getTime();
		//For metrics
		Logging.getInstance()
				.log("Makespan: " + sInstance.TransactionEndTime,
						Logging.METRICS);					
	}

	public static Simulation getInstance() {
		return sInstance;
	}
	
	public Cluster getCluster() {
		return cluster;
	}
}
