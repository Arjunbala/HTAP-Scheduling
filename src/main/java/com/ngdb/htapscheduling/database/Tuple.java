package com.ngdb.htapscheduling.database;

public class Tuple {
	private String mTableName; // name of table
	private Integer mId; // index of primary key
	private Double mMemoryKb; // amount of memory occupied by tuple

	/**
	 * Parameterized constructor
	 * 
	 * @param tableName
	 * @param id
	 */
	public Tuple(String tableName, Integer id, Double memoryKb) {
		mTableName = tableName;
		mId = id;
		mMemoryKb = memoryKb;
	}

	/**
	 * Get table name corresponding to this tuple
	 * 
	 * @return String, indicating table name
	 */
	public String getTableName() {
		return mTableName;
	}

	/**
	 * Get ID of this tuple wrt. primary key of the table
	 * 
	 * @return Integer, indicating the ID
	 */
	public Integer getId() {
		return mId;
	}
	
	public Double getMemory() {
		return mMemoryKb;
	}
}