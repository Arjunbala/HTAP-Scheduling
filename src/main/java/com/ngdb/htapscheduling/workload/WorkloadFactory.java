package com.ngdb.htapscheduling.workload;

import org.json.simple.JSONObject;

import com.ngdb.htapscheduling.config.ConfigUtils;

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
	
	public Workload getWorkloadGenerator(JSONObject config) {
		String workloadType = ConfigUtils.getAttributeValue(config,
				"workload_type");
		switch(workloadType) {
		case "test":
			return new TestWorkload();
		case "ssb":
			return new SSB(config);
		}
		return null;
	}
}
