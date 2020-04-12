package com.ngdb.htapscheduling.cluster.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.ngdb.htapscheduling.cluster.WorkingSet;
import com.ngdb.htapscheduling.database.Tuple;

public class RandomMemoryManagement implements MemoryManagement {
	
	
	@Override
	public List<Tuple> MemoryManagementByPolicy(WorkingSet gpuWorkingSet, WorkingSet cpuWorkingSet, 
			List<Tuple> transactionReadSet, Double memoryNeeded) {
		List<Tuple> tupleList = gpuWorkingSet.getTupleList();
		int size = tupleList.size();
		Random rand = new Random();
		List<Tuple> evictTupleList = new ArrayList<>();
		
		while (memoryNeeded > 0) {
			int tupleNumber = rand.nextInt(size);
			
			if (tupleList.get(tupleNumber).getEvictionBit()==false) {
				if (transactionReadSet.indexOf(tupleList.get(tupleNumber)) == -1) {
					evictTupleList.add(tupleList.get(tupleNumber));
					tupleList.get(tupleNumber).setEvictionBit();
					memoryNeeded -= tupleList.get(tupleNumber).getMemory();
				}
			}
		}
		return evictTupleList;
	}
}
