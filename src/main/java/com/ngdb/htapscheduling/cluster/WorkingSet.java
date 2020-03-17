package com.ngdb.htapscheduling.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.database.Tuple;

public class WorkingSet {

	private List<Tuple> mTuples; // List of tuples belonging to this working set
	private Map<Tuple, Integer> mVersionSet; // Version of each tuple in this
												// working set

	/**
	 * Default constructor
	 */
	public WorkingSet() {
		mTuples = new ArrayList<Tuple>();
		mVersionSet = new HashMap<Tuple, Integer>();
	}

	public boolean hasTuple(Tuple t) {
		return mVersionSet.containsKey(t) ? true : false;
	}

	/**
	 * Add a tuple to this working set
	 * 
	 * @param Tuple, indicating the tuple to be added
	 * @param Integer, indicating the version number of this tuple
	 * @return true, if tuple was added, false otherwise
	 */
	public boolean addTuple(Tuple t, Integer version) {
		if (!mTuples.contains(t)) {
			mTuples.add(t);
			mVersionSet.put(t, version);
			return true;
		}
		return false;
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