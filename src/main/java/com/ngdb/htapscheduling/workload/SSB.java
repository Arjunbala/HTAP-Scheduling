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
		
		transactions = new ArrayList<Transaction>();
	}

	@Override
	public List<Tuple> getTupleList() {
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		int seed = 1;
		double olap_prob = 0.5;
		double tx0_prob = 0.25;
		double tx1_prob = 0.25;
		double tx2_prob = 0.25;
		//double tx3_prob = 0.25;
		double read_prob = 0.5;
		//double write_prob = 1 - read_prob;
		int min_tuples_oltp = 5;
		int max_tuples_oltp = 20;
		int totalReqs = (int) (mRps*mDurationSeconds);
		int inter_arrival_time_ms = (int)(1000.0/mRps);
		Random rand = new Random(seed);
		int supp_offset = 2556;
		int cust_offset = 2556+2000;
		int part_offset = 2556+2000+30000;
		int lo_offset = 2556+2000+30000+200000;
		int total_tuples = 2556+2000+30000+200000+6001215;
	
		for (int j=0; j<totalReqs; j++) {
			double submissionTime = (double)j*inter_arrival_time_ms*1.0;
			double r1 = rand.nextDouble();
			boolean isOlap;
			if (r1 < olap_prob)
				isOlap = true;
			else isOlap = false;

			if (isOlap) {  //OLAP
				Transaction t = new Transaction(j+1, submissionTime, 1.0, 5.0, isOlap);
				double r2 = rand.nextDouble();
				int tx_type;
				if (r2 < tx0_prob)
					tx_type = 0;
				else if (r2 < tx1_prob + tx0_prob)
					tx_type = 1;
				else if (r2 < tx2_prob + tx1_prob + tx0_prob)
					tx_type = 2;
				else
					tx_type = 3;

				if (tx_type==0) {
					for (int i=0;i<2556;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<114550;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
					t.setOutputSize(1*11/1024.0);
					t.setmCPURunningTimeEstimateMs(8828.0);
					t.setmGPURunningTimeEstimateMs(542.0);
				
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
					t.setOutputSize(280*32/1024.0);
					t.setmCPURunningTimeEstimateMs(3690.0);
					t.setmGPURunningTimeEstimateMs(226.0);
				
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
					t.setOutputSize(150*47/1024.0);
					t.setmCPURunningTimeEstimateMs(22108.0);
					t.setmGPURunningTimeEstimateMs(1357.0);
				
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
					t.setOutputSize(35*32/1024.0);
					t.setmCPURunningTimeEstimateMs(20978.0);
					t.setmGPURunningTimeEstimateMs(1287.0);
				
				}
				else {
					System.out.println("Invalid tx type!!!!");
					System.exit(0);	
				}
				transactions.add(t);
			}
			else { //OLTP
				Transaction t = new Transaction(j+1, submissionTime, 1.0, 5.0, isOlap);
				int num_tuples = rand.nextInt(max_tuples_oltp-min_tuples_oltp+1) + min_tuples_oltp;

				for (int i=0; i<num_tuples; i++) {
					double r3 = rand.nextDouble();
					if (r3 < read_prob) {
						int rand_read = rand.nextInt(total_tuples);
						t.addToReadSet(mTuples.get(rand_read));
					}
					else {
						int rand_write = rand.nextInt(total_tuples);
						t.addToWriteSet(mTuples.get(rand_write));
					}
				}	
			
				transactions.add(t);
			}
		}
		return transactions;
	}
}

