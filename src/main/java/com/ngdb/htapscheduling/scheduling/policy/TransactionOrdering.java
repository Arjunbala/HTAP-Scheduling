package com.ngdb.htapscheduling.scheduling.policy;

import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;

public interface TransactionOrdering {
	public List<TransactionExecutionContext> orderTransactionsByPolicy(List<Transaction> transactionList);
}