package com.ngdb.htapscheduling.scheduling.policy;

import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;

public class HeuristicTransactionOrdering implements TransactionOrdering {

	@Override
	public List<TransactionExecutionContext> orderTransactionsByPolicy(
			List<Transaction> transactionList) {
		// TODO Auto-generated method stub
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
			// execution context
			// as CPU
			// Estimate when these transactions are likely to complete
			// Enqueue a epoch start event with the remaining transactions
		}
		return null;
	}

	private List<Transaction> getOltpConflicts(
			List<Transaction> allTransactions) {
		// TODO
		return null;
	}
}