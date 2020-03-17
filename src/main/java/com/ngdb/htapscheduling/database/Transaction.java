package com.ngdb.htapscheduling.database;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

	private Integer mTransactionId; // Unique XID assigned to each transaction
	private Double mGPURunningTimeEstimateMs; // Measured runtime estimation on
												// GPU
	private Double mCPURunningTimeEstimateMs; // Measured runtime estimation on
												// CPU
	private boolean mIsOlap; // whether this transaction is read-only
	private List<Tuple> mReadSet; // read set for this transaction
	private List<Tuple> mWriteSet; // write set for this transaction

	/**
	 * Parameterized constructor for transaction
	 * 
	 * @param trxId
	 * @param gpuRunningTimeEstimate
	 * @param cpuRunningTimeEstimate
	 * @param isOlap
	 */
	public Transaction(Integer trxId, Double gpuRunningTimeEstimateMs,
			Double cpuRunningTimeEstimateMs, boolean isOlap) {
		mTransactionId = trxId;
		mGPURunningTimeEstimateMs = gpuRunningTimeEstimateMs;
		mCPURunningTimeEstimateMs = cpuRunningTimeEstimateMs;
		mIsOlap = isOlap;
		// Init empty read and write sets
		mReadSet = new ArrayList<Tuple>();
		mWriteSet = new ArrayList<Tuple>();
	}

	/**
	 * Add an item to the read set
	 * 
	 * @param Tuple, indicating the tuple to be added
	 * @return boolean, indicating if tuple was successfully added or not
	 */
	public boolean addToReadSet(Tuple t) {
		if (!mReadSet.contains(t)) {
			mReadSet.add(t);
			return true;
		}
		return false;
	}

	/**
	 * Remove an item from the read set
	 * 
	 * @param Tuple, indicating the tuple to be removed
	 * @return boolean, indicating if tuple was successfully removed or not
	 */
	public boolean removeFromReadSet(Tuple t) {
		if (mReadSet.contains(t)) {
			mReadSet.remove(t);
			return true;
		}
		return false;
	}

	/**
	 * Add an item to the write set
	 * 
	 * @param Tuple, indicating the tuple to be added
	 * @return boolean, indicating if tuple was successfully added or not
	 */
	public boolean addToWriteSet(Tuple t) {
		if (!mWriteSet.contains(t)) {
			mWriteSet.add(t);
			return true;
		}
		return false;
	}

	/**
	 * Remove an item from the write set
	 * 
	 * @param Tuple, indicating the tuple to be removed
	 * @return boolean, indicating if tuple was successfully removed or not
	 */
	public boolean removeFromWriteSet(Tuple t) {
		if (mWriteSet.contains(t)) {
			mWriteSet.remove(t);
			return true;
		}
		return false;
	}
}