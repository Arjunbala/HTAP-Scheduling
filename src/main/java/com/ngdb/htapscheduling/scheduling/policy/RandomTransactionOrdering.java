package com.ngdb.htapscheduling.scheduling.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;

public class RandomTransactionOrdering implements TransactionOrdering {

	private Random mDeviceSelectionRandom;
	private Random mGPUSlotSelectionRandom;

	public RandomTransactionOrdering() {
		mDeviceSelectionRandom = new Random(0);
		mGPUSlotSelectionRandom = new Random(0);
	}

	@Override
	public List<TransactionExecutionContext> orderTransactionsByPolicy(
			List<Transaction> transactionList) {
		List<TransactionExecutionContext> executionContexts = new ArrayList<>();
		for (Transaction t : transactionList) {
			// Choose CPU or GPU at random
			if (mDeviceSelectionRandom.nextBoolean()) {
				// CPU
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("cpu", 0)));
			} else {
				// GPU
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("gpu",
								mGPUSlotSelectionRandom.nextInt(
										Simulation.getInstance().getCluster()
												.getNumGPUSlots()))));
			}
		}
		return executionContexts;
	}
}