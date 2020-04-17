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
		mTuples.add(new Tuple("test", 1, 1024.0, 1));
		mTuples.add(new Tuple("test", 2, 1024.0, 1));
		mTuples.add(new Tuple("test", 3, 1024.0, 1));
		mTuples.add(new Tuple("test", 4, 1024.0, 1));
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		List<Transaction> transactions = new ArrayList<Transaction>();
		Transaction t1 = new Transaction(1, 0.0, 1.0, 5.0, true);
		t1.addToReadSet(mTuples.get(0));
		t1.setOutputSize(1024.0);
		transactions.add(t1);
		Transaction t2 = new Transaction(2, 0.0, 1.0, 5.0, true);
		t2.addToReadSet(mTuples.get(3));
		t2.setOutputSize(1024.0);
		transactions.add(t2);
		Transaction t3 = new Transaction(3, 0.0, 1.0, 5.0, true);
		t3.addToReadSet(mTuples.get(2));
		t3.setOutputSize(1024.0);
		transactions.add(t3);
		Transaction t4 = new Transaction(4, 11.0, 1.0, 5.0, false);
		t4.addToReadSet(mTuples.get(2));
		t4.addToWriteSet(mTuples.get(2));
		t4.setOutputSize(1024.0);
		transactions.add(t4);
		Transaction t5 = new Transaction(5, 19.0, 1.0, 5.0, true);
		t5.addToReadSet(mTuples.get(3));
		t5.setOutputSize(1024.0);
		transactions.add(t5);
		Transaction t6 = new Transaction(6, 19.0, 1.0, 5.0, true);
		t6.addToReadSet(mTuples.get(3));
		t6.setOutputSize(1024.0);
		transactions.add(t6);
		return transactions;
	}
	
}
