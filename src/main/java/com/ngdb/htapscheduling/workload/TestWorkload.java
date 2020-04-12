package com.ngdb.htapscheduling.workload;

import java.util.ArrayList;
import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;

public class TestWorkload implements Workload {

	List<Tuple> mTuples;
	@Override
	public List<Tuple> getTupleList() {
		mTuples = new ArrayList<Tuple>();
		mTuples.add(new Tuple("test", 1, 10.0, 1));
		mTuples.add(new Tuple("test", 2, 10.0, 1));
		mTuples.add(new Tuple("test", 3, 10.0, 1));
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		List<Transaction> transactions = new ArrayList<Transaction>();
		Transaction t = new Transaction(1, 0.0, 1.0, 5.0, true);
		t.addToReadSet(mTuples.get(0));
		t.addToReadSet(mTuples.get(1));
		transactions.add(t);
		return transactions;
	}
	
}
