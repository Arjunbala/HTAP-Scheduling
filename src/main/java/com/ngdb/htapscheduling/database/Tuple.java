package com.ngdb.htapscheduling.database;
import java.util.BitSet;

public class Tuple {
	private String mTableName; // name of table
	private Integer mId; // index of primary key
	private Double mMemoryB; // amount of memory occupied by tuple -- will be more convenient if expressed in bytes (Krati)
	private Integer mNumCols; //num of columns in the table
	private Boolean mEvictionBit; //eviction bit for memory Management
	/**
	 * Parameterized constructor
	 * 
	 * @param tableName
	 * @param id
	 */
	public Tuple(String tableName, Integer id, Double memoryB, Integer num_cols ) {
		mTableName = tableName;
		mId = id;
		mMemoryB = memoryB;
		mNumCols = num_cols;
		mEvictionBit = false;
	}

	/**
	 * Get table name corresponding to this tuple
	 * 
	 * @return String, indicating table name
	 */
	public String getTableName() {
		return mTableName;
	}
	
	@Override
	public String toString() {
		return mTableName + ":" + Integer.toString(mId);
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
		return mMemoryB/1024.0;
	}
	
	public Boolean getEvictionBit() {
		return mEvictionBit;
	}
	
	public void setEvictionBit() {
		mEvictionBit = true;
	}

}
