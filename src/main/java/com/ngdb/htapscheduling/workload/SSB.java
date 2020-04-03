package com.ngdb.htapscheduling.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;

public class SSB implements Workload {

	List<Tuple> mTuples;
	List<Transaction> transactions;
	int mRps = 5;
	int mDurationSeconds = 20;
	public SSB() {
		mTuples = new ArrayList<Tuple>();
		for (int i=0; i<2556; i++)
			mTuples.add(new Tuple("date", i+1, 115.0, 17)); //17*9=119

		for (int i=0; i<2000; i++)
			mTuples.add(new Tuple("supplier", i+1, 139.0, 7));//140

		for (int i=0; i<30000; i++)
			mTuples.add(new Tuple("customer", i+1, 123.0, 8));//120
		
		for (int i=0; i<200000; i++)
			mTuples.add(new Tuple("part", i+1, 113.0, 9)); //117
		
		for (int i=0; i<6001215; i++)
			mTuples.add(new Tuple("lineorder", i+1, 107.0, 17)); //17*6=102
		
		List<Transaction> transactions = new ArrayList<Transaction>();
	}

	@Override
	public List<Tuple> getTupleList() {
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		int totalReqs = (int) (mRps*mDurationSeconds);
		int inter_arrival_time_ms = (int)(1000.0/mRps);
		Random rand = new Random();
		int supp_offset = 2556;
		int cust_offset = 2556+2000;
		int part_offset = 2556+2000+30000;
		int lo_offset = 2556+2000+30000+200000;
		int total_tuples = 2556+2000+30000+200000+6001215;
	
		for (int j=0; j<totalReqs; j++) {
			double submissionTime = (double)j*inter_arrival_time_ms*1.0;
			boolean isOlap = rand.nextBoolean();
			if (isOlap) {  //OLAP
				Transaction t = new Transaction(j+1, submissionTime, 1.0, 5.0, isOlap);
				int tx_type = rand.nextInt(4);
				if (tx_type==0) {
					for (int i=0;i<2556;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<114550;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
				
				}
				else if (tx_type==1) {
					for (int i=0;i<2556;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<2000;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<14186;i++) //adding 'part' table
						t.addToReadSet(mTuples.get(part_offset+i));
					for (int i=0;i<425580;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
				
				}
				else if (tx_type==2) {
					for (int i=0;i<2556;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<2000;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<30000;i++) //adding 'customer' table
						t.addToReadSet(mTuples.get(cust_offset+i));
					for (int i=0;i<1347435;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
				
				}
				else if (tx_type==3) {
					for (int i=0;i<2556;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<2000;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<30000;i++) //adding 'customer' table
						t.addToReadSet(mTuples.get(cust_offset+i));
					for (int i=0;i<200000;i++) //adding 'part' table
						t.addToReadSet(mTuples.get(part_offset+i));
					for (int i=0;i<1133502;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
				
				}
				else {
					System.out.println("Invalid tx type!!!!");
					System.exit(0);	
				}
				transactions.add(t);
			}
			else { //OLTP
				Transaction t = new Transaction(j+1, submissionTime, 1.0, 5.0, isOlap);
				int num_reads = rand.nextInt(10);	
				int num_writes = rand.nextInt(10);	
				for (int i=0; i<num_reads; i++) {
					int rand_read = rand.nextInt(total_tuples);
					t.addToReadSet(mTuples.get(rand_read));
				}	
				for (int i=0; i<num_writes; i++) {
					int rand_write = rand.nextInt(total_tuples);
					t.addToWriteSet(mTuples.get(rand_write));
				}	
			
				transactions.add(t);
			}
		}
		return transactions;
	}
}

