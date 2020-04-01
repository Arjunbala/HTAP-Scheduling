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
		mTuples.add(new Tuple("test", 1, 10.0));
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		List<Transaction> transactions = new ArrayList<Transaction>();
		Transaction t = new Transaction(1, 0.0, 1.0, 5.0, true);
		t.addToReadSet(mTuples.get(0));
		transactions.add(t);
		return transactions;
	}
	
}

public class SSB implements Workload {

	List<Tuple> mTuples;
	List<Transaction> transactions;
	@Override
	public SSB() {
		mTuples = new ArrayList<Tuple>();
		for (int i=0; i<2556; i++)
			mTuples.add(new Tuple("date", i+1, 115.0, 17));

		for (int i=0; i<2000; i++)
			mTuples.add(new Tuple("supplier", i+1, 139.0, 7));

		for (int i=0; i<30000; i++)
			mTuples.add(new Tuple("customer", i+1, 123.0, 8));
		
		for (int i=0; i<200000; i++)
			mTuples.add(new Tuple("part", i+1, 113.0, 9));
		
		for (int i=0; i<6001215; i++)
			mTuples.add(new Tuple("lineorder", i+1, 107.0, 17));
		
		List<Transaction> transactions = new ArrayList<Transaction>();
		//OLAP queries

		int supp_offset = 2556;
		int cust_offset = 2556+2000;
		int part_offset = 2556+2000+30000;
		int lo_offset = 2556+2000+30000+200000;
		//Q1
		Transaction t1 = new Transaction(1, 0.0, 1.0, 5.0, true);
		for (int i=0;i<2556;i++) //adding 'date' table
			t1.addToReadSet(mTuples.get(i));
		for (int i=0;i<114550;i++) //adding 'lineorder' table
			t1.addToReadSet(mTuples.get(lo_offset+i));
		
		transactions.add(t1);
		
		//Q2
		Transaction t2 = new Transaction(2, 0.0, 1.0, 5.0, true);
		for (int i=0;i<2556;i++) //adding 'date' table
			t2.addToReadSet(mTuples.get(i));
		for (int i=0;i<2000;i++) //adding 'supplier' table
			t2.addToReadSet(mTuples.get(supp_offset+i));
		for (int i=0;i<14186;i++) //adding 'part' table
			t2.addToReadSet(mTuples.get(part_offset+i));
		for (int i=0;i<425580;i++) //adding 'lineorder' table
			t2.addToReadSet(mTuples.get(lo_offset+i));
		
		transactions.add(t2);
		
		//Q3
		Transaction t3 = new Transaction(3, 0.0, 1.0, 5.0, true);
		t3.addToReadSet(mTuples.get(0));
		
		for (int i=0;i<2556;i++) //adding 'date' table
			t3.addToReadSet(mTuples.get(i));
		for (int i=0;i<2000;i++) //adding 'supplier' table
			t3.addToReadSet(mTuples.get(supp_offset+i));
		for (int i=0;i<30000;i++) //adding 'customer' table
			t3.addToReadSet(mTuples.get(cust_offset+i));
		for (int i=0;i<1347435;i++) //adding 'lineorder' table
			t3.addToReadSet(mTuples.get(lo_offset+i));
		transactions.add(t3);

		//Q4
		Transaction t4 = new Transaction(4, 0.0, 1.0, 5.0, true);
		t4.addToReadSet(mTuples.get(0));
		
		for (int i=0;i<2556;i++) //adding 'date' table
			t4.addToReadSet(mTuples.get(i));
		for (int i=0;i<2000;i++) //adding 'supplier' table
			t4.addToReadSet(mTuples.get(supp_offset+i));
		for (int i=0;i<30000;i++) //adding 'customer' table
			t4.addToReadSet(mTuples.get(cust_offset+i));
		for (int i=0;i<200000;i++) //adding 'part' table
			t4.addToReadSet(mTuples.get(part_offset+i));
		for (int i=0;i<1133502;i++) //adding 'lineorder' table
			t4.addToReadSet(mTuples.get(lo_offset+i));
		transactions.add(t4);

		//OLTP queries
	}

	@Override
	public List<Tuple> getTupleList() {
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		return transactions;
	}
	
}
