package com.ngdb.htapscheduling.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;

public class TransactionExecutor {
	
	// Used by the transaction execution management
	Integer numTransactionsUsingCPU;
	Double earliestNextCPUAvailability;
	Map<Integer, Double> earliestNextGPUAvailability;
	List<Tuple> cpuReadLocks;
	List<Tuple> cpuWriteLocks;

	public TransactionExecutor() {
		numTransactionsUsingCPU = 0;
		earliestNextCPUAvailability = 0.0;
		earliestNextGPUAvailability = new HashMap<Integer, Double>();
		for (int i = 0; i < Simulation.getInstance().getCluster()
				.getNumGPUSlots(); i++) {
			earliestNextGPUAvailability.put(i, 0.0);
		}
		cpuReadLocks = new ArrayList<Tuple>();
		cpuWriteLocks = new ArrayList<Tuple>();
	}
	
	public Integer startTransactionExecution(Transaction transaction, Location location) {
		return 0;
	}
	
	public void endTransactionExecution(Transaction transaction, Location location) {
	}
}