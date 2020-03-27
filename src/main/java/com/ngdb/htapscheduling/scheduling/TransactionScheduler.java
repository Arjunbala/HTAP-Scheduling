package com.ngdb.htapscheduling.scheduling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;
import com.ngdb.htapscheduling.events.EventQueue;
import com.ngdb.htapscheduling.events.TransactionEndEvent;
import com.ngdb.htapscheduling.events.TransactionStartEvent;
import com.ngdb.htapscheduling.scheduling.policy.OrderingPolicy;
import com.ngdb.htapscheduling.scheduling.policy.TransactionOrdering;
import com.ngdb.htapscheduling.scheduling.policy.TransactionOrderingPolicyFactory;

public class TransactionScheduler {

	public static TransactionScheduler sInstance = null;
	private TransactionOrdering transactionOrderer;
	private TransactionExecutor executor;

	// Used while deciding initial schedule of transaction
	Double cpuAvailableTime;
	Map<Integer, Double> gpuAvailableTime;

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
		executor = new TransactionExecutor();
	}

	// Transaction scheduling management strategies below
	public void scheduleTransactionExecution(
			List<Transaction> transactionList) {
		List<TransactionExecutionContext> orderAndLocationToExecute = transactionOrderer
				.orderTransactionsByPolicy(transactionList);

		// First schedule CPU transactions optimistically -- executor will take care of concurrency control
		for (TransactionExecutionContext context : orderAndLocationToExecute) {
			if (context.getLocation().getDevice().equals("cpu")) {
				EventQueue.getInstance()
				.enqueueEvent(new TransactionStartEvent(Simulation.getInstance().getTime(),
						context.getTransaction(),
						context.getLocation()));
			}
		}

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
		Integer status = executor.startTransactionExecution(transaction, location);
		if(status == 0) {
			// successful, enqueue end transaction event
			Double endTime = executor.getEndTime(transaction, location);
			EventQueue.getInstance().enqueueEvent(new TransactionEndEvent(
					Simulation.getInstance().getTime(), endTime, transaction, location));
		} else if (status == 1) {
			// cpu currently unavailable, schedule start transaction for when CPU becomes available
			EventQueue.getInstance()
			.enqueueEvent(new TransactionStartEvent(executor.getCPUNextAvailableTime(),
					transaction,
					location));
		} else if (status == 2) {
			// GPU currently unavailable, schedule start transaction for when GPU becomes available
			EventQueue.getInstance()
			.enqueueEvent(new TransactionStartEvent(executor.getGPUNextAvailableTime(location.getId()),
					transaction,
					location));
		} else if (status == 3) {
			// Read/write set conflict, find out earliest time when all locks will be available
			EventQueue.getInstance()
			.enqueueEvent(new TransactionStartEvent(executor.getEarliestLockAvailability(transaction),
					transaction,
					location));
		}
	}

	public void endTransaction(Transaction transaction, Location location) {
		executor.endTransactionExecution(transaction, location);
	}
}