package com.ngdb.htapscheduling.database;

public class Tuple {
	private String mTableName; // name of table
	private Integer mId; // index of primary key

	/**
	 * Parameterized constructor
	 * 
	 * @param tableName
	 * @param id
	 */
	public Tuple(String tableName, Integer id) {
		mTableName = tableName;
		mId = id;
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
}