package com.ngdb.htapscheduling.events;

import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.scheduling.TransactionScheduler;

public class TransactionEndEvent extends Event {

	Transaction transaction;
	Location location;
	Double startTime;
	Double endTime;
	
	public TransactionEndEvent(double startTime, double timestamp, Transaction transaction, Location location) {
		super(timestamp);
		this.transaction = transaction;
		this.location = location;
		this.startTime = startTime;
		this.endTime = timestamp;
		setPriority(Event.EventType.TRANSACTION_END);
	}
	
	@Override
	public void handleEvent() {
		// TODO: Record metrics
		super.handleEvent();
		TransactionScheduler.getInstance().endTransaction(transaction, location);
	}
}