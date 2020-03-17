package com.ngdb.htapscheduling.events;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Global event queue to manage cluster level events.
 */
public class EventQueue {
	
	private static EventQueue sInstance = null;
	public static TreeSet<Event> mEventQueue;
	
	/**
	 * Private constructor
	 */
	private EventQueue() {
		mEventQueue = new TreeSet<Event>(new ClusterEventComparator());
	}
	
	/**
	 * Get an instance of the cluster event queue
	 * @return instance of ClusterEventQueue
	 */
	public static EventQueue getInstance() {
		if(sInstance == null) {
			sInstance = new EventQueue();
		}
		return sInstance;
	}
	
	/**
	 * Enqueue a cluster event to the cluster event queue
	 * @param event
	 */
	public void enqueueEvent(Event event) {
		mEventQueue.add(event);
	}
	
	/**
	 * Return the current size of event size
	 * @return integer, corresponding to number of events
	 */
	public int getNumberEvents() {
		return mEventQueue.size();
	}
	
	/**
	 * Start event processing
	 */
	public void start() {
		while(!mEventQueue.isEmpty()) {
			Event event = mEventQueue.pollFirst();
			event.handleEvent();
		}
	}
	
	/**
	 * Comparator for cluster events
	 */
	private class ClusterEventComparator implements Comparator<Event> {

		@Override
		public int compare(Event e1, Event e2) {
			// First check timestamps
			if(e1.getTimestamp() != e2.getTimestamp()) {
				if(e1.getTimestamp() < e2.getTimestamp()) {
					return -1;
				} else {
					return 1;
				}
			}
			// If timestamps are same, then priority comes into picture
			if(e1.getPriority() != e2.getPriority()) {
				return e1.getPriority() - e2.getPriority();
			}
			// If everything is same, then random ordering
			return 1;
		}
	}
}