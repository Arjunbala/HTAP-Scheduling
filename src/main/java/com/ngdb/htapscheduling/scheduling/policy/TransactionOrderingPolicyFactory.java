package com.ngdb.htapscheduling.scheduling.policy;

public class TransactionOrderingPolicyFactory {
	private static TransactionOrderingPolicyFactory sInstance = null;
	
	public static TransactionOrderingPolicyFactory getInstance() {
		if(sInstance == null) {
			sInstance = new TransactionOrderingPolicyFactory();
		}
		return sInstance;
	}
	
	public TransactionOrdering createOrderingPolicy(OrderingPolicy policy) {
		switch(policy) {
		case RANDOM:
			return new RandomTransactionOrdering();
		case HEURISTIC:
			return new HeuristicTransactionOrdering();
		}
		return null;
	}
}