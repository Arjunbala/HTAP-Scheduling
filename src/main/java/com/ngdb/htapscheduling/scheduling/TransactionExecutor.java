package com.ngdb.htapscheduling.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.cluster.PCIeUtils;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.database.TupleContext;

public class TransactionExecutor {

	// Used by the transaction execution management
	Integer numTransactionsUsingCPU;
	Map<Transaction, Double> transactionCompletionTimeCPU;
	Map<Integer, Double> transactionCompletionTimeGPU;
	List<TupleContext> cpuReadLocks;
	List<TupleContext> cpuWriteLocks;

	public TransactionExecutor() {
		numTransactionsUsingCPU = 0;
		transactionCompletionTimeCPU = new HashMap<Transaction, Double>();
		transactionCompletionTimeGPU = new HashMap<Integer, Double>();
		cpuReadLocks = new ArrayList<TupleContext>();
		cpuWriteLocks = new ArrayList<TupleContext>();
	}

	/**
	 * 
	 * @param transaction
	 * @param location
	 * @return 0, if transaction successfully started execution 1, CPU is
	 *         unavailable currently 2, GPU is unavailable currently 3,
	 *         read/write set conflict on CPU
	 */
	public Integer startTransactionExecution(Transaction transaction,
			Location location) {
		if (location.getDevice().equals("cpu")) {
			// Check if CPU is available
			// TODO: Arbit policy -- revisit
			if (numTransactionsUsingCPU <= Simulation.getInstance().getCluster()
					.getCores() / 4) {
				// proceed -- check that there are no conflicts in read and
				// write set
				List<TupleContext> conflicts = checkConflicts(transaction);
				if (conflicts.size() > 0) {
					// there is a conflict
					return 3;
				} else {
					// Can execute on CPU
					updateReadAndWriteLocks(transaction, location, true);
					numTransactionsUsingCPU++;
					Double transEndTime = Simulation.getInstance().getTime();
					if (location.getDevice().equals("cpu")) {
						transEndTime += transaction.getCPUExecutionTime();
					} else {
						transEndTime += transaction.getGPURunningTime();
					}
					transactionCompletionTimeCPU.put(transaction, transEndTime);
					return 0;
				}
			} else {
				// CPU currently unavailable
				return 1;
			}
		} else {
			// Check if the particular GPU is available
			if (transactionCompletionTimeGPU.get(location.getId()) == null) {
				// check if all tuples in read set already reside on GPU
				// If not, initiate a transfer
				Double totalDataToTransferKb = 0.0;
				for (Tuple t : transaction.getReadSet()) {

					if (!Simulation.getInstance().getCluster()
							.doesGPUHaveLatestTupleVersion(location.getId(), t,
									Simulation.getInstance().getCluster()
											.latestTupleVersion(t))) {
						totalDataToTransferKb += t.getMemory();
						//GIRI
						//Simulation.getInstance().getCluster().getGPUWorkingSet(location.getId())
						//		.getLastAccessed().put(t, Simulation.getInstance().getTime());
						
						Simulation.getInstance().getCluster()
								.migrateTupleToGPU(t, location.getId(), transaction.getReadSet());
					}
				}
				for (Tuple t : transaction.getReadSet()) {
					Simulation.getInstance().getCluster().getGPUWorkingSet(location.getId())
					.getLastAccessed().put(t, Simulation.getInstance().getTime());
				}
				Double transactionCompletionTime = PCIeUtils
						.getDeviceToHostTransferTime(
								transaction.getOutputSize())
						+ PCIeUtils.getHostToDeviceTransferTime(
								totalDataToTransferKb)
						+ transaction.getGPURunningTime();
				transactionCompletionTimeGPU.put(location.getId(),
						Simulation.getInstance().getTime()
								+ transactionCompletionTime);
				return 0;
			} else {
				// GPU in use
				return 2;
			}
		}
	}

	public void endTransactionExecution(Transaction transaction,
			Location location) {
		if (location.getDevice().equals("cpu")) {
			updateReadAndWriteLocks(transaction, location, false);
			numTransactionsUsingCPU--;
			transactionCompletionTimeCPU.remove(transaction);
			// Update versions for tuples in write set
			for (Tuple t : transaction.getWriteSet()) {
				Simulation.getInstance().getCluster().mCPUWorkingSet
						.incrementTupleVersion(t);
			}
		} else {
			// GPU
			transactionCompletionTimeGPU.remove(location.getId());
		}
	}
	
	public Double getEndTime(Transaction transaction, Location loc) {
		if(loc.getDevice().equals("cpu")) {
			return transactionCompletionTimeCPU.get(transaction);
		} else {
			return transactionCompletionTimeGPU.get(loc.getId());
		}
	}
	
	public Double getCPUNextAvailableTime() {
		if(numTransactionsUsingCPU <= Simulation.getInstance().getCluster()
					.getCores() / 4) {
			return Simulation.getInstance().getTime();
		} else {
			// return time of earliest transaction completion
			Double earliest = Double.POSITIVE_INFINITY;
			for(Double time : transactionCompletionTimeCPU.values()) {
				if(time < earliest) {
					earliest = time;
				}
			}
			return earliest;
		}
	}
	
	public Double getEarliestLockAvailability(Transaction transaction) {
		Double earliest = 0.0;
		for(Tuple t : transaction.getReadSet()) {
			for(TupleContext tc : cpuWriteLocks) {
				if(tc.getTuple().equals(t)) {
					if(earliest < tc.getReleaseTime()) {
						earliest = tc.getReleaseTime();
					}
				}
			}
		}
		for(Tuple t : transaction.getWriteSet()) {
			for(TupleContext tc : cpuWriteLocks) {
				if(tc.getTuple().equals(t)) {
					if(earliest < tc.getReleaseTime()) {
						earliest = tc.getReleaseTime();
					}
				}
			}
			for(TupleContext tc : cpuReadLocks) {
				if(tc.getTuple().equals(t)) {
					if(earliest < tc.getReleaseTime()) {
						earliest = tc.getReleaseTime();
					}
				}
			}
		}
		return earliest;
	}
	
	public Double getGPUNextAvailableTime(Integer gpuID) {
		Double time = transactionCompletionTimeGPU.get(gpuID);
		if(time == null) {
			return Simulation.getInstance().getTime();
		}
		return time;
	}

	private List<TupleContext> checkConflicts(Transaction transaction) {
		List<TupleContext> conflicts = new ArrayList<TupleContext>();
		// Examine read set
		for (Tuple t : transaction.getReadSet()) {
			// check if any of these tuples are write locked
			for (TupleContext tc : cpuWriteLocks) {
				if (tc.getTuple().equals(t)) {
					if (!conflicts.contains(tc)) {
						conflicts.add(tc);
					}
				}
			}
		}
		// Examine write set
		for (Tuple t : transaction.getReadSet()) {
			// check if any of the tuples are read or write locked
			for (TupleContext tc : cpuWriteLocks) {
				if (tc.getTuple().equals(t)) {
					if (!conflicts.contains(tc)) {
						conflicts.add(tc);
					}
				}
			}
			for (TupleContext tc : cpuReadLocks) {
				if (tc.getTuple().equals(t)) {
					if (!conflicts.contains(tc)) {
						conflicts.add(tc);
					}
				}
			}
		}
		return conflicts;
	}

	private void updateReadAndWriteLocks(Transaction transaction,
			Location location, boolean isInsert) {
		if (isInsert) {
			handleInsert(transaction, location);
		} else {
			handleRemove(transaction, location);
		}
	}

	private void handleInsert(Transaction transaction, Location location) {
		Double transEndTime = Simulation.getInstance().getTime();
		if (location.getDevice().equals("cpu")) {
			transEndTime += transaction.getCPUExecutionTime();
		} else {
			transEndTime += transaction.getGPURunningTime();
		}
		for (Tuple t : transaction.getReadSet()) {
			boolean tupleAlreadyReadLocked = false;
			for (TupleContext tc : cpuReadLocks) {
				if (tc.getTuple().equals(t)) {
					tupleAlreadyReadLocked = true;
					tc.setReleaseTime(
							Math.max(tc.getReleaseTime(), transEndTime));
				}
			}
			if (!tupleAlreadyReadLocked) {
				cpuReadLocks.add(new TupleContext(t, transEndTime));
			}
		}
		for (Tuple t : transaction.getWriteSet()) {
			cpuWriteLocks.add(new TupleContext(t, transEndTime));
		}
	}

	private void handleRemove(Transaction transaction, Location location) {
		List<TupleContext> entries = new ArrayList<>();
		for (Tuple t : transaction.getReadSet()) {
			for (TupleContext tc : cpuReadLocks) {
				// hack to avoid doing ref counting holders of read locks
				if (tc.getTuple().equals(t)
						&& Double.compare(Simulation.getInstance().getTime(),
								tc.getReleaseTime()) == 0) {
					entries.add(tc);
				}
			}
		}
		for (TupleContext entry : entries) {
			cpuReadLocks.remove(entry);
		}
		entries = new ArrayList<>();
		for (Tuple t : transaction.getWriteSet()) {
			for (TupleContext tc : cpuWriteLocks) {
				if (tc.getTuple().equals(t)) {
					entries.add(tc);
				}
			}
		}
		for (TupleContext entry : entries) {
			cpuWriteLocks.remove(entry);
		}
	}
}