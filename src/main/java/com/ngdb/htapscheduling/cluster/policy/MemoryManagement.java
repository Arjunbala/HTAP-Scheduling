package com.ngdb.htapscheduling.cluster.policy;

import java.util.List;

import com.ngdb.htapscheduling.cluster.WorkingSet;
import com.ngdb.htapscheduling.database.Tuple;

public interface MemoryManagement {
	public List<Tuple> MemoryManagementByPolicy(WorkingSet gpuWorkingSet, WorkingSet cpuWorkingSet,
			List<Tuple> transactionReadSet, Double memoryNeeded);
}


