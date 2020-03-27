package com.ngdb.htapscheduling.events;

import java.util.ArrayList;
import java.util.List;

import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.scheduling.TransactionScheduler;

public class EpochStartEvent extends Event {

	List<Transaction> transactionList;
	Integer epochNumber;
	
	public EpochStartEvent(double timestamp, List<Transaction> transactionList, Integer epochNumber) {
		super(timestamp);
		this.transactionList = new ArrayList<Transaction>(transactionList);
		this.epochNumber = epochNumber;
		setPriority(Event.EventType.EPOCH_START_EVENT);
	}
	
	@Override
	public void handleEvent() {
		TransactionScheduler.getInstance().scheduleTransactionExecution(transactionList);
	}
}