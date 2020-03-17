package com.ngdb.htapscheduling.events;

import com.ngdb.htapscheduling.Simulation;

/**
 * Generic event which will be enqueued into event queue
 */
public abstract class Event {
	private double mTimestamp; // Timestamp of event
	private int mPriority; // When timestamps of two events are same, priority matters.
	
	public enum EventType {
		HIGH, // Highest priority
		LOW // Lowest priority
	}
	
	/**
	 * Constuctor
	 * @param timestamp
	 */
	public Event(double timestamp) {
		mTimestamp = timestamp;
	}
	
	/**
	 * Returns the timestamp of event
	 */
	public double getTimestamp() {
		return mTimestamp;
	}
	
	/**
	 * Event handler for this event.
	 */
	public void handleEvent() {
		Simulation.getInstance().setTime(mTimestamp);
	}
	
	/**
	 * Getter for priority
	 */
	public int getPriority() {
		return mPriority;
	}
	
	/**
	 * Setter for priority
	 */
	protected void setPriority(EventType eventType) {
		mPriority = eventType.ordinal();
	}
}