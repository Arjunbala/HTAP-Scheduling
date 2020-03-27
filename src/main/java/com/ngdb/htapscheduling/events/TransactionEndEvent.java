package com.ngdb.htapscheduling.events;

import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.scheduling.TransactionScheduler;

public class TransactionEndEvent extends Event {

	Transaction transaction;
	Location location;
	
	public TransactionEndEvent(double timestamp, Transaction transaction, Location location) {
		super(timestamp);
		this.transaction = transaction;
		this.location = location;
		setPriority(Event.EventType.TRANSACTION_END);
	}
	
	@Override
	public void handleEvent() {
		TransactionScheduler.getInstance().endTransaction(transaction, location);
	}
}