package com.ngdb.htapscheduling.scheduling.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;
import com.ngdb.htapscheduling.events.EpochStartEvent;
import com.ngdb.htapscheduling.events.EventQueue;

public class HeuristicTransactionOrdering implements TransactionOrdering {

	@Override
	public List<TransactionExecutionContext> orderTransactionsByPolicy(
			List<Transaction> transactionList) {
		// TODO Auto-generated method stub
		List<TransactionExecutionContext> executionContexts = new ArrayList<>();
		// Filter out oltp transactions that conflict with olap
		List<Transaction> conflictingTransactions = getOltpConflicts(
				transactionList);
		// if there are conflicts,
		// return only the conflicting transactions and specify on CPU
		// remaining transactions -- enqueue an event in the future to process
		// them
		if (conflictingTransactions.size() == 0) {
			// case B
		} else {
			// order the conflicting transactions by accept stamp and specify
			// execution context as CPU
			Collections.sort(conflictingTransactions,
					new Comparator<Transaction>() {
						public int compare(Transaction a, Transaction b) {
							int cmp = Double.compare(a.getAcceptStamp(),
									b.getAcceptStamp());
							if (cmp != 0) {
								return cmp;
							}
							// Tie break by transaction ID
							return a.getTransactionId() - b.getTransactionId();
						}
					});
			for (Transaction t : conflictingTransactions) {
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("cpu", 0)));
			}
			// Estimate when these transactions are likely to complete
			Double timeEstimate = getEstimateForTransactions(
					conflictingTransactions);
			// Enqueue a epoch start event with the remaining transactions
			List<Transaction> remainingTransactions = new ArrayList<>();
			for (Transaction t : transactionList) {
				if (!conflictingTransactions.contains(t)) {
					remainingTransactions.add(t);
				}
			}
			EpochStartEvent ev = new EpochStartEvent(
					Simulation.getInstance().getTime() + timeEstimate,
					remainingTransactions, -1);
			EventQueue.getInstance().enqueueEvent(ev);
		}
		return executionContexts;
	}

	private List<Transaction> getOltpConflicts(
			List<Transaction> allTransactions) {
		
		List<Transaction> oltpConflicts = new ArrayList<Transaction>();
		for (Transaction txn : allTransactions) {
			boolean conflict = false;
			for (Tuple t : txn.getWriteSet()) {
			//check conflicts with all other txn's readsets
				for (int i=0; i<allTransactions.size(); i++) {
					if (i != allTransactions.indexOf(txn)) {
						Transaction txn_ = allTransactions.get(i);
						if(txn_.isOlap()) {
							for (Tuple t_ : txn_.getReadSet()) {
								if (t.equals(t_)) {
									conflict = true;
									break;
								}
							}
						}
					}
					if (conflict)
						break;
				}
				if (conflict)
					break;	
			}
			if (conflict)
				oltpConflicts.add(txn);	
		}
		return oltpConflicts;
	}

	private Double getEstimateForTransactions(
			List<Transaction> conflictingTransactions) {
		// TODO
		return 0.0;
	}
}
