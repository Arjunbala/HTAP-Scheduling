package com.ngdb.htapscheduling.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.database.Tuple;

public class WorkingSet {

	private List<Tuple> mTuples; // List of tuples belonging to this working set
	private Map<Tuple, Integer> mVersionSet; // Version of each tuple in this
												// working set
	private Map<Tuple, Double> mLastAccessed; //Time of last access of each tuple in this working set
	/**
	 * Default constructor
	 */
	public WorkingSet() {
		mTuples = new ArrayList<Tuple>();
		mVersionSet = new HashMap<Tuple, Integer>();
		mLastAccessed = new HashMap<Tuple, Double>();
	}

	public boolean hasTuple(Tuple t) {
		return mVersionSet.containsKey(t) ? true : false;
	}

	//GIRI
	public List<Tuple> getTupleList() {
		return mTuples;
	}
	
	public Map<Tuple, Integer> getVersionSet() {
		return mVersionSet;
	}
	
	public Map<Tuple, Double> getLastAccessed() {
		return mLastAccessed;
	}
	
	public void setLastAccessed(Tuple t, Double timestamp) {
		mLastAccessed.put(t, timestamp);
	}
	
	/**
	 * Add a tuple to this working set
	 * 
	 * @param Tuple, indicating the tuple to be added
	 * @param Integer, indicating the version number of this tuple
	 * @return true, if tuple was added, false otherwise
	 */
	public boolean addTuple(Tuple t, Integer version, Boolean isGpu) {
		mTuples.add(t);
		mVersionSet.put(t, version);
		//GIRI
		if(isGpu) {
			mLastAccessed.put(t, Simulation.getInstance().getTime());
		}
		return true;
	}
	
	public void addTuples(List<Tuple> tuples, Integer version, Boolean isGPU) {
		mTuples = tuples;
		for(Tuple t : mTuples) {
			mVersionSet.put(t, version);
			if(isGPU) {
				mLastAccessed.put(t, Simulation.getInstance().getTime());
			}
		}
	}

	/**
	 * Remove a tuple from this working set
	 * 
	 * @param Tuple, indicating the tuple to be removed
	 * @return true, if tuple was removed, false otherwise
	 */
	public boolean removeTuple(Tuple t) {
		if (mTuples.contains(t)) {
			mTuples.remove(t);
			mVersionSet.remove(t);
			mLastAccessed.remove(t);
		}
		return false;
	}

	/**
	 * Increment the version number associated with this tuple
	 * 
	 * @param Tuple, indicating the tuple for which version should be
	 *        incremented
	 * @return true, if increment was successful, false otherwise.
	 */
	public boolean incrementTupleVersion(Tuple t) {
		if (mVersionSet.containsKey(t)) {
			Integer version = mVersionSet.get(t);
			mVersionSet.put(t, version + 1);
			return true;
		}
		return false;
	}

	/**
	 * Get the version of this tuple
	 * 
	 * @param Tuple, indicating the tuple
	 * @return Integer, version number of the tuple if exists, -1 otherwise
	 */
	public Integer getTupleVersion(Tuple t) {
		return mVersionSet.containsKey(t) ? mVersionSet.get(t) : -1;
	}
	
	
}