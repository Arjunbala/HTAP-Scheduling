package com.ngdb.htapscheduling.database;
import java.util.HashMap;
import java.util.Map;

// TODO: Row and columnar format?

public class TableMemory {
	private Map<String, Double> mTupleMemory;
	private static TableMemory sInstance = null;
	
	public static TableMemory getInstance() {
		if(sInstance == null) {
			sInstance = new TableMemory();
		}
		return sInstance;
	}
	
	private TableMemory() {
		mTupleMemory = new HashMap<String, Double>();
	}
	
	public void addTableStats(String tableName, Double memory) {
		mTupleMemory.put(tableName, memory);
	}
	
	public Double getMemoryForTuple(String tableName) {
		return mTupleMemory.get(tableName);
	}
}