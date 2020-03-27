package com.ngdb.htapscheduling.cluster;

import java.util.HashMap;
import java.util.Map;

import com.ngdb.htapscheduling.database.Tuple;

/**
 * Cluster consists of a CPU with 'n' cores and 'm' GPU slots
 */
public class Cluster {
	private Integer mNumCPUCores; // Number of CPU cores in the cluster
	private Integer mNumGPUSlots; // Number of GPUs in this server
	private WorkingSet mCPUWorkingSet; // CPU Working Set - assume infinite memory here
	private Map<Integer, WorkingSet> mGPUWorkingSet; // Mapping between GPU and its working set
	private Map<Integer, Double> mAvailableGPUMemoryKB; // Mapping between GPU and available memory
	
	/**
	 * Parameterized constructor
	 * @param numCPUCores
	 * @param numGPUs
	 */
	public Cluster(Integer numCPUCores, Integer numGPUs, Double gpuMemoryKB) {
		mNumCPUCores = numCPUCores;
		mNumGPUSlots = numGPUs;
		mCPUWorkingSet = new WorkingSet();
		mGPUWorkingSet = new HashMap<Integer, WorkingSet>();
		mAvailableGPUMemoryKB = new HashMap<Integer, Double>();
		for(int i=0;i<mNumGPUSlots;i++) {
			mGPUWorkingSet.put(i, new WorkingSet());
			mAvailableGPUMemoryKB.put(i, gpuMemoryKB);
		}
	}
	
	public Integer getCores() {
		return mNumCPUCores;
	}
	
	public Integer getNumGPUSlots() {
		return mNumGPUSlots;
	}
	
	public boolean addTupleToCPU(Tuple t, Integer version) {
		return mCPUWorkingSet.addTuple(t, version);
	}
	
	public boolean removeTupleFromCPU(Tuple t) {
		return mCPUWorkingSet.removeTuple(t);
	}
	
	public boolean addTupleToGPU(Tuple t, Integer version, Integer gpuID) {
		if(gpuID >= mNumGPUSlots) {
			return false;
		}
		// TODO: Memory checks and eviction policy
		return mGPUWorkingSet.get(gpuID).addTuple(t, version);
	}
	
	public boolean removeTupleFromGPU(Tuple t, Integer gpuID) {
		if(gpuID >= mNumGPUSlots) {
			return false;
		}
		// TODO: Update memory
		return mGPUWorkingSet.get(gpuID).removeTuple(t);
	}
	
	public boolean migrateTupleToGPU(Tuple t, Integer gpuID) {
		if(mGPUWorkingSet.get(gpuID).getTupleVersion(t) < mCPUWorkingSet.getTupleVersion(t)) {
			// TODO: Memory check and eviction policy
			mGPUWorkingSet.get(gpuID).removeTuple(t);
			mGPUWorkingSet.get(gpuID).addTuple(t, mCPUWorkingSet.getTupleVersion(t));
			return true;
		}
		return false;
	}
}