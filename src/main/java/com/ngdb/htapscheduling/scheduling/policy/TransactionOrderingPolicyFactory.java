package com.ngdb.htapscheduling.scheduling.policy;

import org.json.simple.JSONObject;
import com.ngdb.htapscheduling.config.ConfigUtils;

public class TransactionOrderingPolicyFactory {
	private static TransactionOrderingPolicyFactory sInstance = null;
	
	public static TransactionOrderingPolicyFactory getInstance() {
		if(sInstance == null) {
			sInstance = new TransactionOrderingPolicyFactory();
		}
		return sInstance;
	}
	
	public TransactionOrdering createOrderingPolicy(JSONObject config) {
		String policy = ConfigUtils.getAttributeValue(config, "policy_type");
		switch(policy) {
			case "random":
				return new RandomTransactionOrdering();
			case "heuristic":
				return new HeuristicTransactionOrdering(config);
		}
		return null;
	}
}
