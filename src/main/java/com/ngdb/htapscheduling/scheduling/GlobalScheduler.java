package com.ngdb.htapscheduling.scheduling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.events.EpochStartEvent;
import com.ngdb.htapscheduling.events.EventQueue;

/**
 * GlobalScheduler splits transactions into epochs and schedules them
 */
public class GlobalScheduler {

	private static GlobalScheduler sInstance = null;
	private Double epochTime;
	private List<Transaction> transactionList;

	public static GlobalScheduler createInstance(Double epochTime) {
		if (sInstance != null) {
			// already have an instance
			return null;
		}
		sInstance = new GlobalScheduler(epochTime);
		return sInstance;
	}

	public static GlobalScheduler getInstance() {
		return sInstance;
	}

	public GlobalScheduler(Double epochTime) {
		this.epochTime = epochTime;
		transactionList = new ArrayList<Transaction>();
	}

	public void addTransaction(Transaction t) {
		transactionList.add(t);
	}

	public void startExecution() {
		// Sort the transaction list by accept stamp
		Collections.sort(transactionList, new Comparator<Transaction>() {
			public int compare(Transaction t1, Transaction t2) {
				int compare = Double.compare(t1.getAcceptStamp(),
						t2.getAcceptStamp());
				// Tie break by transaction ID
				return (compare == 0)
						? (t1.getTransactionId() - t2.getTransactionId())
						: compare;
			}
		});
		Integer epochNumber = 0;
		List<Transaction> transactionsInEpoch = new ArrayList<Transaction>();
		for (Transaction t : transactionList) {
			if (Double.compare(t.getAcceptStamp(),
					(epochNumber + 1) * epochTime) < 0) {
				// still in current epoch
				transactionsInEpoch.add(t);
			} else {
				// move to next epoch
				EventQueue.getInstance()
						.enqueueEvent(new EpochStartEvent(
								(epochNumber + 1) * epochTime,
								transactionsInEpoch, epochNumber));
				transactionsInEpoch = new ArrayList<>();
				epochNumber += 1; // advance epoch
				transactionsInEpoch.add(t);
			}
		}
	}
}