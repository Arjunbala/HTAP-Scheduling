package com.ngdb.htapscheduling.database;

import com.ngdb.htapscheduling.Simulation;

public class Location {
	
	private String device;
	private Integer id;
	
	public Location(String device, Integer id) {
		assert(device.equals("gpu") || device.equals("cpu"));
		this.device = device;
		if(this.device == "gpu") {
			assert(this.id >= 0 && this.id < Simulation.getInstance().getCluster().getNumGPUSlots());
		}
		this.id = id;
	}
	
	public String getDevice() {
		return device;
	}
	
	public Integer getId() {
		return id;
	}
}