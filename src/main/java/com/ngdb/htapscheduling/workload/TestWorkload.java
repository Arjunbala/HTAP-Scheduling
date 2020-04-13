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
		Transaction t1 = new Transaction(1, 0.0, 1.0, 5.0, false);
		t1.addToReadSet(mTuples.get(0));
		t1.addToWriteSet(mTuples.get(1));
		t1.setOutputSize(10.0);
		transactions.add(t1);
		Transaction t2 = new Transaction(2, 1.0, 1.0, 5.0, true);
		t2.addToReadSet(mTuples.get(0));
		t2.addToReadSet(mTuples.get(1));
		t2.setOutputSize(10.0);
		transactions.add(t2);
		return transactions;
	}
	
}
