package com.ngdb.htapscheduling.workload;

import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;

public interface Workload {
	public List<Tuple> getTupleList();
	public List<Transaction> getTransactionList();
}