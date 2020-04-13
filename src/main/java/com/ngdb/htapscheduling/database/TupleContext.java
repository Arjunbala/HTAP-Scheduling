package com.ngdb.htapscheduling.database;

public class TupleContext {
	private Tuple mTuple;
	private Double mExpectedReleaseTime;
	private Transaction mTransaction;

	public TupleContext(Tuple t, Double releaseTime, Transaction txn) {
		mTuple = t;
		mExpectedReleaseTime = releaseTime;
		mTransaction = txn;
	}

	public Tuple getTuple() {
		return mTuple;
	}

	public Double getReleaseTime() {
		return mExpectedReleaseTime;
	}

	public Transaction getHoldingTransaction() {
		return mTransaction;
	}

	public void setReleaseTime(Double releaseTime) {
		mExpectedReleaseTime = releaseTime;
	}

	@Override
	public String toString() {
		return "Tuple " + mTuple.getTableName() + "-" + mTuple.getId()
				+ " released at " + mExpectedReleaseTime
				+ " currently held by transaction "
				+ mTransaction.getTransactionId();
	}
}