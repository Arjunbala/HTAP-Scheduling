package com.ngdb.htapscheduling.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.json.simple.JSONObject;

import com.ngdb.htapscheduling.config.ConfigUtils;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;

public class SSB_new implements Workload {

	List<Tuple> mTuples;
	List<Transaction> transactions;
	private int mRps;
	private int mDurationSeconds;
	private double olap_prob;
	private double tx0_prob;
	private double tx1_prob;
	private double tx2_prob;
	//private double read_prob;
	private int min_tuples_oltp;
	private int max_tuples_oltp;

	public SSB_new(JSONObject config) {
		JSONObject ssbConfig = ConfigUtils.getJsonValue(config,	"workload_config");

		mRps = Integer.parseInt(ConfigUtils.getAttributeValue(ssbConfig, "mRps"));
		mDurationSeconds = Integer.parseInt(ConfigUtils.getAttributeValue(ssbConfig, "mDurationSeconds"));
		min_tuples_oltp = Integer.parseInt(ConfigUtils.getAttributeValue(ssbConfig, "min_tuples_oltp"));
		max_tuples_oltp = Integer.parseInt(ConfigUtils.getAttributeValue(ssbConfig, "max_tuples_oltp"));
		olap_prob = Double.parseDouble(ConfigUtils.getAttributeValue(ssbConfig, "olap_prob"));
		tx0_prob = Double.parseDouble(ConfigUtils.getAttributeValue(ssbConfig, "tx0_prob"));
		tx1_prob = Double.parseDouble(ConfigUtils.getAttributeValue(ssbConfig, "tx1_prob"));
		tx2_prob = Double.parseDouble(ConfigUtils.getAttributeValue(ssbConfig, "tx2_prob"));
		//read_prob = Double.parseDouble(ConfigUtils.getAttributeValue(ssbConfig, "read_prob"));

		mTuples = new ArrayList<Tuple>();
		for (int i=0; i<255; i++)
			mTuples.add(new Tuple("date", i+1, 115.0, 17)); //17*9=119

		for (int i=0; i<200; i++)
			mTuples.add(new Tuple("supplier", i+1, 139.0, 7));//140

		for (int i=0; i<3000; i++)
			mTuples.add(new Tuple("customer", i+1, 123.0, 8));//120
		
		for (int i=0; i<5000; i++)
			mTuples.add(new Tuple("part", i+1, 113.0, 9)); //117
		
		for (int i=0; i<60012; i++)
			mTuples.add(new Tuple("lineorder", i+1, 107.0, 17)); //17*6=102
		
		transactions = new ArrayList<Transaction>();
	}

	@Override
	public List<Tuple> getTupleList() {
		return mTuples;
	}

	@Override
	public List<Transaction> getTransactionList() {
		int totalReqs = (int) (mRps*mDurationSeconds);
		int inter_arrival_time_ms = (int)(1000.0/mRps);
		Random rand = new Random(0);
		int supp_offset = 255;
		int cust_offset = 255+200;
		int part_offset = 255+200+3000;
		int lo_offset = 255+200+3000+5000;
		int total_tuples = 255+200+3000+5000+60012;
	
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
					for (int i=0;i<255;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<1145;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
					t.setOutputSize(1*11/1024.0);
					t.setmCPURunningTimeEstimateMs(8828.0);	
					t.setmGPURunningTimeEstimateMs(542.0);
				
				}
				else if (tx_type==1) {
					for (int i=0;i<255;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<200;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<355;i++) //adding 'part' table
						t.addToReadSet(mTuples.get(part_offset+i));
					for (int i=0;i<4255;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
					t.setOutputSize(280*32/1024.0);
					t.setmCPURunningTimeEstimateMs(3690.0);	
					t.setmGPURunningTimeEstimateMs(226.0);
				
				}
				else if (tx_type==2) {
					for (int i=0;i<255;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<200;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<3000;i++) //adding 'customer' table
						t.addToReadSet(mTuples.get(cust_offset+i));
					for (int i=0;i<13474;i++) //adding 'lineorder' table
						t.addToReadSet(mTuples.get(lo_offset+i));
					t.setOutputSize(150*47/1024.0);
					t.setmCPURunningTimeEstimateMs(22108.0);	
					t.setmGPURunningTimeEstimateMs(1357.0);
				
				}
				else if (tx_type==3) {
					for (int i=0;i<255;i++) //adding 'date' table
						t.addToReadSet(mTuples.get(i));
					for (int i=0;i<200;i++) //adding 'supplier' table
						t.addToReadSet(mTuples.get(supp_offset+i));
					for (int i=0;i<3000;i++) //adding 'customer' table
						t.addToReadSet(mTuples.get(cust_offset+i));
					for (int i=0;i<5000;i++) //adding 'part' table
						t.addToReadSet(mTuples.get(part_offset+i));
					for (int i=0;i<11335;i++) //adding 'lineorder' table
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
				Transaction t = new Transaction(j+1, submissionTime, 1.0, 0.0, isOlap);
				int num_tuples = rand.nextInt(max_tuples_oltp-min_tuples_oltp+1) + min_tuples_oltp;
				int num_read_tuples = 0;
				int num_write_tuples = 0;

				for (int i=0; i<num_tuples; i++) {
					int rand_read = rand.nextInt(total_tuples);
					t.addToReadSet(mTuples.get(rand_read));
					t.addToWriteSet(mTuples.get(rand_read));
				}	
				t.setmCPURunningTimeEstimateMs(0.004*num_write_tuples + 0.9922);
				transactions.add(t);
			}
		}
		return transactions;
	}
}

