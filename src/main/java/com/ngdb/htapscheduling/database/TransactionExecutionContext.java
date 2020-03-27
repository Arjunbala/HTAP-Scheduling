package com.ngdb.htapscheduling.database;

public class TransactionExecutionContext {
	
	private Transaction transaction;
	private Location location;
	
	public TransactionExecutionContext(Transaction transaction, Location location) {
		this.transaction = transaction;
		this.location = location;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}
	
	public Location getLocation() {
		return location;
	}
}