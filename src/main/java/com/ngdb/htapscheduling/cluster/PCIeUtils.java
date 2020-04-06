package com.ngdb.htapscheduling.cluster;

public class PCIeUtils {
	public static Double getHostToDeviceTransferTime(Double sizeKB) {
		if (sizeKB < 16384.0)
			return 1.0;
		else
			return Math.ceil(0.0001205*sizeKB-0.3629);
	}
	
	public static Double getDeviceToHostTransferTime(Double sizeKB) {
		if (sizeKB <= 2048)
			return 1.0;
		else
			return Math.ceil(0.0003490*sizeKB+0.1357);
	}
}