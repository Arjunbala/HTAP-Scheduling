package com.ngdb.htapscheduling.workload;

public class WorkloadFactory {
	private static WorkloadFactory sInstance = null;
	
	public static WorkloadFactory getInstance() {
		if(sInstance == null) {
			sInstance = new WorkloadFactory();
		}
		return sInstance;
	}
	
	private WorkloadFactory() {
	}
	
	public Workload getWorkloadGenerator(String workloadType) {
		switch(workloadType) {
		case "test":
			return new TestWorkload();
		}
		return null;
	}
}