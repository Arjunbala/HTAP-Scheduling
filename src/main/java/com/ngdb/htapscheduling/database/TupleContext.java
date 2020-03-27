package com.ngdb.htapscheduling.database;

public class TupleContext {
	private Tuple mTuple;
	private Double mExpectedReleaseTime;
	
	public TupleContext(Tuple t, Double releaseTime) {
		mTuple = t;
		mExpectedReleaseTime = releaseTime;
	}
	
	public Tuple getTuple() {
		return mTuple;
	}
	
	public Double getReleaseTime() {
		return mExpectedReleaseTime;
	}
	
	public void setReleaseTime(Double releaseTime) {
		mExpectedReleaseTime = releaseTime;
	}
}