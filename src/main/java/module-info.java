module htap {
	exports com.ngdb.htapscheduling;
	exports com.ngdb.htapscheduling.database;
	exports com.ngdb.htapscheduling.cluster;
	exports com.ngdb.htapscheduling.scheduling;
	exports com.ngdb.htapscheduling.scheduling.policy;
	exports com.ngdb.htapscheduling.workload;
	exports com.ngdb.htapscheduling.events;

	requires java.logging;
}