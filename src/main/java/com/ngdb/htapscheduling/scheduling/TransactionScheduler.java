package com.ngdb.htapscheduling.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.events.EventQueue;
import com.ngdb.htapscheduling.events.TransactionStartEvent;
import com.ngdb.htapscheduling.scheduling.policy.OrderingPolicy;
import com.ngdb.htapscheduling.scheduling.policy.TransactionOrdering;
import com.ngdb.htapscheduling.scheduling.policy.TransactionOrderingPolicyFactory;

public class TransactionScheduler {

	public static TransactionScheduler sInstance = null;
	private TransactionOrdering transactionOrderer;

	// Used while deciding initial schedule of transaction
	Double cpuAvailableTime;
	Map<Integer, Double> gpuAvailableTime;

	// Used by the transaction execution management
	Integer numTransactionsUsingCPU;
	Double earliestNextCPUAvailability;
	Map<Integer, Double> earliestNextGPUAvailability;
	List<Tuple> cpuReadLocks;
	List<Tuple> cpuWriteLocks;

	public static TransactionScheduler getInstance() {
		if (sInstance == null) {
			sInstance = new TransactionScheduler();
		}
		return sInstance;
	}

	private TransactionScheduler() {
		// TODO: Make configurable
		transactionOrderer = TransactionOrderingPolicyFactory.getInstance()
				.createOrderingPolicy(OrderingPolicy.RANDOM);
		cpuAvailableTime = 0.0;
		gpuAvailableTime = new HashMap<Integer, Double>();
		for (int i = 0; i < Simulation.getInstance().getCluster()
				.getNumGPUSlots(); i++) {
			gpuAvailableTime.put(i, 0.0);
		}
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

	// Transaction scheduling management strategies below
	public void scheduleTransactionExecution(
			List<Transaction> transactionList) {
		List<TransactionExecutionContext> orderAndLocationToExecute = transactionOrderer
				.orderTransactionsByPolicy(transactionList);

		// First schedule CPU transactions
		// TODO: Think -- optimistic or pessimistic approach?

		// Now, schedule GPU transactions, GPU wise
		for (int i = 0; i < Simulation.getInstance().getCluster()
				.getNumGPUSlots(); i++) {
			// GPUs can execute one transaction at a time
			for (TransactionExecutionContext context : orderAndLocationToExecute) {
				if (context.getLocation().getDevice().equals("gpu")
						&& context.getLocation().getId() == i) {
					Double startTime = gpuAvailableTime.get(i);
					EventQueue.getInstance()
							.enqueueEvent(new TransactionStartEvent(startTime,
									context.getTransaction(),
									context.getLocation()));
					gpuAvailableTime.put(i, startTime
							+ context.getTransaction().getGPURunningTime());
				}
			}
		}
	}

	// Transaction execution management APIs below
	public void startTransaction(Transaction transaction, Location location) {
		
	}

	public void endTransaction(Transaction transaction, Location location) {

	}
}