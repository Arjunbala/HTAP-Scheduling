package com.ngdb.htapscheduling.cluster.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.cluster.WorkingSet;

public class LRUMemoryManagement implements MemoryManagement {
	
	public int getLowestTimeStamp (List<Tuple> tupleList, Map<Tuple, Double> lastAccessed, 
			List<Tuple> transactionReadSet) {
		
		double lowestTimeStamp = lastAccessed.get(tupleList.get(0));
		int indexTuple = 0;
		for (int i = 1; i < tupleList.size(); i++) {
			//If it was earlier accessed, not already added to eviction list,
			//and taking care of the case where we don't evict something from the readSet
			if (lastAccessed.get(tupleList.get(i)) < lowestTimeStamp 
					&& tupleList.get(i).getEvictionBit() == false 
					&& transactionReadSet.indexOf(tupleList.get(i)) == -1) {
				lowestTimeStamp = lastAccessed.get(tupleList.get(i));
				indexTuple = i;
			}
		}
		//Do the check for whether tuple at index 0 is part of readSet
		//What about the case where we are unable to evict anything?
		if (indexTuple == 0 && transactionReadSet.indexOf(tupleList.get(0)) == -1) {
			return -1; //Might just return 0?
		}
			
		return indexTuple;
	}
	
	@Override
	public List<Tuple> MemoryManagementByPolicy(WorkingSet gpuWorkingSet, WorkingSet cpuWorkingSet,
			List<Tuple> transactionReadSet, Double memoryNeeded) {
		
		List<Tuple> tupleList = gpuWorkingSet.getTupleList();
		Map<Tuple, Double> lastAccessed = gpuWorkingSet.getLastAccessed();
		//Convert Map to a list and then sort it TODO Giri
		List<Tuple> evictTupleList = new ArrayList<>();

		while (memoryNeeded > 0) {
			int lowestTimeStampTupleIndex = getLowestTimeStamp(tupleList, lastAccessed, transactionReadSet);
			if (lowestTimeStampTupleIndex == -1) {
				break;
			}
			tupleList.get(lowestTimeStampTupleIndex).setEvictionBit();
			evictTupleList.add(tupleList.get(lowestTimeStampTupleIndex));
			memoryNeeded -= tupleList.get(lowestTimeStampTupleIndex).getMemory();
		}
		
		return evictTupleList;
	}
	
}
