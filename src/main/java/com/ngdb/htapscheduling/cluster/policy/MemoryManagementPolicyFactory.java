package com.ngdb.htapscheduling.cluster.policy;

public class MemoryManagementPolicyFactory {
	private static MemoryManagementPolicyFactory sInstance = null;
	
	public static MemoryManagementPolicyFactory getInstance() {
		if(sInstance == null) {
			sInstance = new MemoryManagementPolicyFactory();
		}
		return sInstance;
	}
	
	public MemoryManagement createMemoryManagementPolicy(MemoryManagementPolicy policy) {
		switch(policy) {
		case RANDOM:
			return new RandomMemoryManagement();
		case LRU:
			return new LRUMemoryManagement();
		case TWOLEVEL:
			return new TwoLevelMemoryManagement();
		}
		return null;
	}
}