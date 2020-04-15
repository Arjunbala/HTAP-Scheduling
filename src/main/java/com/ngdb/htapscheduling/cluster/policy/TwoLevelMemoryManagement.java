package com.ngdb.htapscheduling.cluster.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.ngdb.htapscheduling.cluster.WorkingSet;
import com.ngdb.htapscheduling.database.Tuple;

public class TwoLevelMemoryManagement implements MemoryManagement {
	
	public int getLowestTimeStamp (List<Tuple> tupleList, Map<Tuple, Double> lastAccessed,
			List<Tuple> transactionReadSet) {
		
		double lowestTimeStamp = lastAccessed.get(tupleList.get(0));
		int indexTuple = 0;
		for (int i=1; i < tupleList.size(); i++) {
			if (lastAccessed.get(tupleList.get(i)) < lowestTimeStamp 
					&& tupleList.get(i).getEvictionBit() == false 
					&& transactionReadSet.indexOf(tupleList.get(i)) == -1) {
				lowestTimeStamp = lastAccessed.get(tupleList.get(i));
				indexTuple = i;
			}
		}
		return indexTuple;
	}
	
	@Override
	public List<Tuple> MemoryManagementByPolicy(WorkingSet gpuWorkingSet, WorkingSet cpuWorkingSet,
			List<Tuple> transactionReadSet, Double memoryNeeded) {
		List<Tuple> tupleList = gpuWorkingSet.getTupleList();
		Map<Tuple, Integer> gpuVersionSet = gpuWorkingSet.getVersionSet();
		Map<Tuple, Double> gpuLastAccessed = gpuWorkingSet.getLastAccessed();
		List<Tuple> evictTupleList = new ArrayList<>();
		
		for (Tuple t: tupleList) {
			if (gpuVersionSet.get(t) < cpuWorkingSet.getTupleVersion(t)) {
				t.setEvictionBit();
				evictTupleList.add(t);
				memoryNeeded -= t.getMemory();
			}
			//We actually try to end up evicting all tuples that are out of version on GPU
			//We just need to uncomment this line if we are only evicting as much as we need.
			//if (memoryNeeded > 0)
			//	continue;
		}
		
		if (memoryNeeded > 0) {			
			while (memoryNeeded > 0) {
				int lowestTimeStampTupleIndex = getLowestTimeStamp(tupleList, 
						gpuLastAccessed, transactionReadSet);
				tupleList.get(lowestTimeStampTupleIndex).setEvictionBit();
				evictTupleList.add(tupleList.get(lowestTimeStampTupleIndex));
				memoryNeeded -= tupleList.get(lowestTimeStampTupleIndex).getMemory();
			}
		}
		
		return evictTupleList;
	}
}
