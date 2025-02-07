package com.ngdb.htapscheduling.cluster;

import org.json.simple.JSONObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Logging;
import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.cluster.policy.MemoryManagement;
import com.ngdb.htapscheduling.cluster.policy.MemoryManagementPolicy;
import com.ngdb.htapscheduling.cluster.policy.MemoryManagementPolicyFactory;
import com.ngdb.htapscheduling.config.ConfigUtils;
import com.ngdb.htapscheduling.database.Tuple;

/**
 * Cluster consists of a CPU with 'n' cores and 'm' GPU slots
 */
public class Cluster {
	private Integer mNumCPUCores; // Number of CPU cores in the cluster
	private Integer mNumGPUSlots; // Number of GPUs in this server
	public WorkingSet mCPUWorkingSet; // CPU Working Set - assume infinite
										// memory here
	private Map<Integer, WorkingSet> mGPUWorkingSet; // Mapping between GPU and
														// its working set
	private Map<Integer, Double> mAvailableGPUMemoryKB; // Mapping between GPU
														// and available memory
	private MemoryManagement memoryManagementPolicy;

	/**
	 * Parameterized constructor
	 * 
	 * @param numCPUCores
	 * @param numGPUs
	 */
	public Cluster(JSONObject clusterConfig, JSONObject memMgmtConfig) {
		mNumCPUCores = Integer.parseInt(ConfigUtils.getAttributeValue(clusterConfig, "numCPUCores")); 
		mNumGPUSlots = Integer.parseInt(ConfigUtils.getAttributeValue(clusterConfig, "numGPUs")); 
		double gpuMemoryKB = Double.parseDouble(ConfigUtils.getAttributeValue(clusterConfig, "gpuMemoryKB")); 
		mCPUWorkingSet = new WorkingSet();
		mGPUWorkingSet = new HashMap<Integer, WorkingSet>();
		mAvailableGPUMemoryKB = new HashMap<Integer, Double>();
		for (int i = 0; i < mNumGPUSlots; i++) {
			mGPUWorkingSet.put(i, new WorkingSet());
			mAvailableGPUMemoryKB.put(i, gpuMemoryKB);
		}
		String memMgmtPolicyType = ConfigUtils.getAttributeValue(memMgmtConfig, "policy_type");
		switch(memMgmtPolicyType) {
			case "random":
				memoryManagementPolicy = MemoryManagementPolicyFactory.getInstance()
					.createMemoryManagementPolicy(MemoryManagementPolicy.RANDOM);
				break;
			case "lru":
				memoryManagementPolicy = MemoryManagementPolicyFactory.getInstance()
					.createMemoryManagementPolicy(MemoryManagementPolicy.LRU);
				break;
			case "two_level":
				memoryManagementPolicy = MemoryManagementPolicyFactory.getInstance()
					.createMemoryManagementPolicy(MemoryManagementPolicy.TWOLEVEL);
				break;
		}
	}

	public Integer getCores() {
		return mNumCPUCores;
	}

	public Integer getNumGPUSlots() {
		return mNumGPUSlots;
	}

	public boolean addTupleToCPU(Tuple t, Integer version) {
		return mCPUWorkingSet.addTuple(t, version, false);
	}
	
	public void addTuplesToCPU(List<Tuple> tuples, Integer version) {
		mCPUWorkingSet.addTuples(tuples, version, false);
	}

	public boolean removeTupleFromCPU(Tuple t) {
		return mCPUWorkingSet.removeTuple(t);
	}

	/*
	 * public boolean addTupleToGPU(Tuple t, Integer version, Integer gpuID) {
	 * if(gpuID >= mNumGPUSlots) { return false; } // TODO: Memory checks and
	 * eviction policy return mGPUWorkingSet.get(gpuID).addTuple(t, version); }
	 * 
	 * public boolean removeTupleFromGPU(Tuple t, Integer gpuID) { if(gpuID >=
	 * mNumGPUSlots) { return false; } // TODO: Update memory return
	 * mGPUWorkingSet.get(gpuID).removeTuple(t); }
	 */

	public boolean migrateTupleToGPU(Tuple t, Integer gpuID,
			List<Tuple> transactionReadSet) {
		if (mGPUWorkingSet.get(gpuID).getTupleVersion(t) < mCPUWorkingSet
				.getTupleVersion(t)) {
			// TODO: Memory check and eviction policy
			if (mGPUWorkingSet.get(gpuID).getTupleVersion(t) == -1) {
				if (mAvailableGPUMemoryKB.get(gpuID) >= t.getMemory()) {
					mAvailableGPUMemoryKB.put(gpuID,
							mAvailableGPUMemoryKB.get(gpuID) - t.getMemory());
				} else {
					Double memoryNeededOnGPU = t.getMemory()
							- mAvailableGPUMemoryKB.get(gpuID);
					Logging.getInstance().log("Eviction required to free memory " + memoryNeededOnGPU, Logging.INFO);
					List<Tuple> evictTuplesList = memoryManagementPolicy
							.MemoryManagementByPolicy(mGPUWorkingSet.get(gpuID),
									mCPUWorkingSet, transactionReadSet,
									memoryNeededOnGPU);
					for (Tuple tt : evictTuplesList) {
						Logging.getInstance().log("Evicting tuple " + tt.toString() , Logging.INFO);
						mAvailableGPUMemoryKB.put(gpuID,
								mAvailableGPUMemoryKB.get(gpuID)
										+ tt.getMemory());
						mGPUWorkingSet.get(gpuID).getTupleList().remove(tt);
					}
					// perform eviction and shit here.
				}
			}
			mGPUWorkingSet.get(gpuID).removeTuple(t);
			mGPUWorkingSet.get(gpuID).addTuple(t,
					mCPUWorkingSet.getTupleVersion(t), true);
			mGPUWorkingSet.get(gpuID).getLastAccessed().put(t,
					Simulation.getInstance().getTime());
			printGPUWorkingSet(gpuID);
			return true;
		}
		return false;
	}

	public WorkingSet getGPUWorkingSet(Integer gpuID) {
		return mGPUWorkingSet.get(gpuID);
	}

	public boolean doesGPUHaveLatestTupleVersion(Integer gpuID, Tuple t,
			Integer targetVersion) {
		WorkingSet gpuWorkingSet = mGPUWorkingSet.get(gpuID);
		if (gpuWorkingSet.getTupleVersion(t) == targetVersion) {
			return true;
		}
		return false;
	}

	public Integer latestTupleVersion(Tuple t) {
		return mCPUWorkingSet.getTupleVersion(t);
	}

	public void printCPUWorkingSet() {
		/*Logging.getInstance().log("Dumping CPU working set", Logging.DEBUG);
		for (Tuple t : mCPUWorkingSet.getTupleList()) {
			Logging.getInstance()
					.log(t.getTableName() + " " + Integer.toString(t.getId())
							+ " version - " + mCPUWorkingSet.getTupleVersion(t),
							Logging.DEBUG);
		}*/
	}

	public void printGPUWorkingSet(Integer gpuID) {
		/*Logging.getInstance()
				.log("Dumping GPU " + gpuID
						+ " working set, available memory = "
						+ mAvailableGPUMemoryKB.get(gpuID), Logging.DEBUG);
		for (Tuple t : mGPUWorkingSet.get(gpuID).getTupleList()) {
			Logging.getInstance().log(
					t.getTableName() + " " + Integer.toString(t.getId()),
					Logging.DEBUG);
		}*/
	}
}
