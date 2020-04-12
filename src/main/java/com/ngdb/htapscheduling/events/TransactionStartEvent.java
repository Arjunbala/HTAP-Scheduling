package com.ngdb.htapscheduling.events;

import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.scheduling.TransactionScheduler;

public class TransactionStartEvent extends Event {

	Transaction transaction;
	Location location;
	
	public TransactionStartEvent(double timestamp, Transaction transaction, Location location) {
		super(timestamp);
		this.transaction = transaction;
		this.location = location;
		setPriority(Event.EventType.TRANSACTION_START);
	}
	
	@Override
	public void handleEvent() {
		super.handleEvent();
		TransactionScheduler.getInstance().startTransaction(transaction, location);
	}
}