package com.ngdb.htapscheduling.scheduling.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;
import com.ngdb.htapscheduling.events.EpochStartEvent;
import com.ngdb.htapscheduling.events.EventQueue;

public class HeuristicTransactionOrdering implements TransactionOrdering {
	
	//private Map<Transaction, Boolean> map;
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
			List<Transaction> nonConflictingTransactions = new ArrayList<>();
			//List<Transaction> conflictingOLAPTransactions = new ArrayList<>();
			for (Transaction t : transactionList) {
				if (!conflictingTransactions.contains(t)) {
			//		if (map.get(t)) {
			//			conflictingOLAPTransactions.add(t);
			//		}
			//		else {
			//			nonConflictingTransactions.add(t);
			//		}
					nonConflictingTransactions.add(t);
				}
			}

			EpochStartEvent ev1 = new EpochStartEvent(
					Simulation.getInstance().getTime() + timeEstimate,
					nonConflictingTransactions, -1);
			EventQueue.getInstance().enqueueEvent(ev1);

			//EpochStartEvent ev2 = new EpochStartEvent(
			//		Simulation.getInstance().getTime() + timeEstimate,
			//		conflictingOLAPTransactions, -1);
			//EventQueue.getInstance().enqueueEvent(ev2);
		}
		return executionContexts;
	}

	private List<Transaction> getOltpConflicts(
			List<Transaction> allTransactions) {
		
		List<Transaction> oltpConflicts = new ArrayList<Transaction>();
		//map = new HashMap<>();
		//for (Transaction txn : allTransactions) {
		//	if (txn.isOlap())
		//		map.put(txn, false);
		//}

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
		//							map.replace(txn_, true);
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

	//private Double getEstimateForTransactions(
	//		List<Transaction> conflictingTransactions) {
	//	double timeEstimate = 0.0;
	//	int max_threads = Simulation.getInstance().getCluster().getCores() / 4;
	//	int used_threads = 0;
	//	int num_txns = conflictingTransactions.size();
	//	List<Transaction> runningTransactions;
	//	List<Transaction> completedTransactions;
	//	Map <Transaction, Double> stop_time_map = new HashMap<>();
	//	Transaction txn = conflictingTransactions.get(0);
	//	runningTransactions.add(txn);
	//	conflictTransactions.remove(txn);
	//	used_threads++;
	//	timeEstimate += txn.getCPUExecutionTime();
	//	stop_time_map.put(txn,Simulation.getInstance().getTime()+ txn.getCPUExecutionTime());
	//	
	//	while (completedTransaction.size() < num_txns) {
	//		for (Transaction t : conflictingTransactions) {
	//			boolean conflict = false;
	//			if (used_threads < max_threads) {
	//				for (Transaction t_ : runningTransactions) {
	//					if (detect_oltp_conflict(t, t_))
	//				       		conflict = true;
	//				}
	//				if (!conflict)	{
	//					runningTransactions.add(t);
	//					conflictingTransactions.remove(t);
	//					used_threads++;
	//					stop_time_map.put(txn,Simulation.getInstance().getTime()+t.getCPUExecutionTime());
	//					timeEstimate = Math.max(Simulation.getInstance().getTime()+t.getCPUExecutionTime(), timeEstimate);
	//				}
	//			}
	//		}
	//		for (Transaction t : runningTransactions) {
	//			if (Simulation.getInstance().getTime() > stop_map_time.get(t)){
	//				runningTransactions.remove(t);
	//				used_threads--;
	//				completedTransactions.add(t);
	//			}
	//		}
	//	}

	//	return timeEstimate;
	//}

	private Double getEstimateForTransactions(
			List<Transaction> conflictingTransactions) {
		double timeEstimate = 0.0;
		int max_threads = Simulation.getInstance().getCluster().getCores() / 4;
		List<Transaction> oltpConflictingTransactions = new ArrayList<Transaction>();
		for (Transaction txn : conflictingTransactions) {
			int txn_index = conflictingTransactions.indexOf(txn);
			for (int i = txn_index+1; i < conflictingTransactions.size(); i++) {
				Transaction txn_ = conflictingTransactions.get(i);
				if(detect_oltp_conflict(txn, txn_)) {
					oltpConflictingTransactions.add(txn);
					conflictingTransactions.remove(txn_index);
					break;
				}
			}
		}
		
		for (Transaction t : oltpConflictingTransactions) {
			timeEstimate += t.getCPUExecutionTime();
		}

		//pick max_threads txns and get max time
		while (conflictingTransactions.size() > 0) {
			double max_time = 0.0;
			for (int i = 0; i<max_threads && i < conflictingTransactions.size(); i++) {
				max_time = Math.max(max_time, conflictingTransactions.get(i).getCPUExecutionTime());
				conflictingTransactions.remove(i);
			}
			timeEstimate += max_time;
		}
		return timeEstimate;
	}

	private Boolean detect_oltp_conflict (Transaction t1, Transaction t2) {
		for (Tuple t : t1.getWriteSet()) {
			for (Tuple t_ : t2.getWriteSet()) {
				if (t.equals(t_))
					return true;
			}
			for (Tuple t_ : t2.getReadSet()) {
				if (t.equals(t_))
					return true;
			}
		}
		for (Tuple t : t1.getReadSet()) {
			for (Tuple t_ : t2.getWriteSet()) {
				if (t.equals(t_))
					return true;
			}
		}
		return false;
	}
}
