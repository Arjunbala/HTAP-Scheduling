package com.ngdb.htapscheduling.scheduling.policy;

import org.json.simple.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.ngdb.htapscheduling.Logging;
import com.ngdb.htapscheduling.Simulation;
import com.ngdb.htapscheduling.cluster.PCIeUtils;
import com.ngdb.htapscheduling.config.ConfigUtils;
import com.ngdb.htapscheduling.database.Location;
import com.ngdb.htapscheduling.database.Transaction;
import com.ngdb.htapscheduling.database.TransactionExecutionContext;
import com.ngdb.htapscheduling.database.Tuple;
import com.ngdb.htapscheduling.events.EpochStartEvent;
import com.ngdb.htapscheduling.events.EventQueue;

public class HeuristicTransactionOrdering implements TransactionOrdering {

	private Double alpha;
	private Double beta;
	private Random mRandom;

	public HeuristicTransactionOrdering(JSONObject config) {
		mRandom = new Random(0);
		JSONObject heuristicConfig = ConfigUtils.getJsonValue(config,
				"policy_config");
		alpha = Double.parseDouble(
				ConfigUtils.getAttributeValue(heuristicConfig, "alpha"));
		beta = Double.parseDouble(
				ConfigUtils.getAttributeValue(heuristicConfig, "beta"));
	}

	@Override
	public List<TransactionExecutionContext> orderTransactionsByPolicy(
			List<Transaction> transactionList) {
		// TODO Auto-generated method stub
		List<TransactionExecutionContext> executionContexts = new ArrayList<>();
		// Filter out oltp transactions that conflict with olap
		List<Transaction> conflictingTransactions = getOltpConflicts(
				transactionList);
		// if there are conflicts,
		// return only the conflicting transactions and specify on CPU
		// remaining transactions -- enqueue an event in the future to process
		// them
		if (conflictingTransactions.size() == 0) {
			// case B
			// First sort transactions into OLTP and OLAP
			List<Transaction> oltpTransactions = new ArrayList<>();
			List<Transaction> olapTransactions = new ArrayList<>();
			for (Transaction t : transactionList) {
				if (t.isOlap()) {
					olapTransactions.add(t);
				} else {
					oltpTransactions.add(t);
				}
			}
			// for olap transactions now, compute a score towards GPU alignment
			Map<Transaction, Map<Integer, Double>> scores = computeScores(
					olapTransactions);
			Map<Integer, List<Transaction>> gpuExecutionList = new HashMap<>();
			for (int i = 0; i < Simulation.getInstance().getCluster()
					.getNumGPUSlots(); i++) {
				gpuExecutionList.put(i, new ArrayList<>());
			}
			while (true && scores.size() > 0) {
				Double maxScore = -1.0;
				Transaction transWithMaxScore = null;
				Integer bestFitGPUID = -1;
				for (Transaction t : scores.keySet()) {
					for (Integer id : scores.get(t).keySet()) {
						if (Double.compare(scores.get(t).get(id),
								maxScore) > 0) {
							maxScore = scores.get(t).get(id);
							transWithMaxScore = t;
							bestFitGPUID = id;
						}
					}
				}
				scores.remove(transWithMaxScore);
				Integer gpuWithMinAssignment = -1;
				Double gpuWithMinAssignmentTime = Double.MAX_VALUE;
				for (Integer gpuID : gpuExecutionList.keySet()) {
					if (Double.compare(
							getGPUEstimateForTransactions(
									gpuExecutionList.get(gpuID)),
							gpuWithMinAssignmentTime) < 0) {
						gpuWithMinAssignment = gpuID;
						gpuWithMinAssignmentTime = getGPUEstimateForTransactions(
								gpuExecutionList.get(gpuID));
					}
				}
				Logging.getInstance()
						.log("Transaction "
								+ transWithMaxScore.getTransactionId()
								+ " need to choose from gpu " + bestFitGPUID
								+ " or " + gpuWithMinAssignment, Logging.INFO);
				// Pick either depending on beta
				Integer gpuIdToUse;
				if (mRandom.nextDouble() < beta) {
					gpuIdToUse = bestFitGPUID;
				} else {
					gpuIdToUse = gpuWithMinAssignment;
				}
				Logging.getInstance()
						.log("Transaction "
								+ transWithMaxScore.getTransactionId()
								+ " chose gpu " + gpuIdToUse, Logging.INFO);
				Logging.getInstance().log(
						"Speedup of GPU chosen transaction: "
								+ transWithMaxScore.getCPUExecutionTime()
										/ transWithMaxScore.getGPURunningTime(),
						Logging.METRICS);
				gpuExecutionList.get(gpuIdToUse).add(transWithMaxScore);
				if (scores.size() == 0) {
					// all assigned to GPU
					break;
				}
				Double maxGPUExecutionTime = 0.0;
				for (int i = 0; i < Simulation.getInstance().getCluster()
						.getNumGPUSlots(); i++) {
					maxGPUExecutionTime = Math.max(maxGPUExecutionTime,
							getGPUEstimateForTransactions(
									gpuExecutionList.get(i)));
				}
				Double cpuExecutionTime = getEstimateForTransactions(
						new ArrayList<>(scores.keySet()));
				if (Double.compare(maxGPUExecutionTime, cpuExecutionTime) > 0) {
					break;
				}
			}
			for (Transaction t : oltpTransactions) {
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("cpu", 0)));
			}
			for (int i = 0; i < Simulation.getInstance().getCluster()
					.getNumGPUSlots(); i++) {
				List<Transaction> transactionsForGPU = gpuExecutionList.get(i);
				for (Transaction t : transactionsForGPU) {
					executionContexts.add(new TransactionExecutionContext(t,
							new Location("gpu", i)));
				}
			}
			Logging.getInstance()
					.log("CPU transactions have total time estimate: "
							+ getEstimateForTransactions(oltpTransactions),
							Logging.METRICS);
			for (int i = 0; i < Simulation.getInstance().getCluster()
					.getNumGPUSlots(); i++) {
				Logging.getInstance().log(
						"GPU " + i + " has total time estimate: "
								+ getGPUEstimateForTransactions(
										gpuExecutionList.get(i)),
						Logging.METRICS);
			}
			for (Transaction t : scores.keySet()) {
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("cpu", 0)));
			}
			Collections.sort(executionContexts,
					new TransactionExecutionContextComparator());
			Logging.getInstance().log("Logging execution contexts in epoch",
					Logging.INFO);
			for (TransactionExecutionContext tec : executionContexts) {
				Logging.getInstance()
						.log("Transaction "
								+ tec.getTransaction().getTransactionId()
								+ " scheduled to run at "
								+ tec.getLocation().toString(), Logging.INFO);
			}
		} else {
			Logging.getInstance().log("Logging conflicting transactions ",
					Logging.INFO);
			for (Transaction t : conflictingTransactions) {
				Logging.getInstance().log("Transaction " + t.getTransactionId()
						+ " has a conflict", Logging.INFO);
			}
			// order the conflicting transactions by accept stamp and specify
			// execution context as CPU
			Collections.sort(conflictingTransactions,
					new TransactionOrderComparator());
			for (Transaction t : conflictingTransactions) {
				executionContexts.add(new TransactionExecutionContext(t,
						new Location("cpu", 0)));
			}
			// Estimate when these transactions are likely to complete
			Double timeEstimate = getEstimateForTransactions(
					conflictingTransactions);
			Logging.getInstance().log(
					"Conflicting transactions will finish in " + timeEstimate,
					Logging.METRICS);
			// Enqueue a epoch start event with the remaining transactions
			List<Transaction> nonConflictingTransactions = new ArrayList<>();
			for (Transaction t : transactionList) {
				if (!conflictingTransactions.contains(t)) {
					nonConflictingTransactions.add(t);
				}
			}

			EpochStartEvent ev1 = new EpochStartEvent(
					Simulation.getInstance().getTime() + timeEstimate,
					nonConflictingTransactions, -1);
			EventQueue.getInstance().enqueueEvent(ev1);
		}
		return executionContexts;
	}

	private List<Transaction> getOltpConflicts(
			List<Transaction> allTransactions) {

		List<Transaction> oltpConflicts = new ArrayList<Transaction>();

		for (Transaction txn : allTransactions) {
			boolean conflict = false;
			for (Tuple t : txn.getWriteSet()) {
				// check conflicts with all other txn's readsets
				for (int i = 0; i < allTransactions.size(); i++) {
					if (i != allTransactions.indexOf(txn)) {
						Transaction txn_ = allTransactions.get(i);
						if (txn_.isOlap()) {
							for (Tuple t_ : txn_.getReadSet()) {
								if (t.equals(t_)) {
									conflict = true;
									// map.replace(txn_, true);
									break;
								}
							}
						}
					}
					if (conflict)
						break;
				}
				if (conflict)
					break;
			}
			if (conflict)
				oltpConflicts.add(txn);
		}
		return oltpConflicts;
	}

	private Double getEstimateForTransactions(
			List<Transaction> conflictingTransactions) {
		double timeEstimate = 0.0;
		int max_threads = Simulation.getInstance().getCluster().getCores() / 4;
		List<Transaction> oltpConflictingTransactions = new ArrayList<Transaction>();
		List<Transaction> tempTransactions = new ArrayList<Transaction>();

		for (Transaction txn : conflictingTransactions) {
			int txn_index = conflictingTransactions.indexOf(txn);
			tempTransactions.add(txn);
			for (int i = txn_index + 1; i < conflictingTransactions
					.size(); i++) {
				Transaction txn_ = conflictingTransactions.get(i);
				if (detect_conflict(txn, txn_)) {
					oltpConflictingTransactions.add(txn);
					tempTransactions.remove(txn);
					break;
				}
			}
		}

		for (Transaction t : oltpConflictingTransactions) {
			timeEstimate += t.getCPUExecutionTime();
		}

		// pick max_threads txns and get max time
		while (tempTransactions.size() > 0) {
			double max_time = 0.0;
			for (int i = 0; i < max_threads; i++) {
				if (i < tempTransactions.size()) {
					max_time = Math.max(max_time,
							tempTransactions.get(i).getCPUExecutionTime());
					tempTransactions.remove(i);
				}
			}
			timeEstimate += max_time;
		}
		return timeEstimate;
	}

	private Boolean detect_conflict(Transaction t1, Transaction t2) {
		for (Tuple t : t1.getWriteSet()) {
			for (Tuple t_ : t2.getWriteSet()) {
				if (t.equals(t_))
					return true;
			}
			for (Tuple t_ : t2.getReadSet()) {
				if (t.equals(t_))
					return true;
			}
		}
		for (Tuple t : t1.getReadSet()) {
			for (Tuple t_ : t2.getWriteSet()) {
				if (t.equals(t_))
					return true;
			}
		}
		return false;
	}

	private Double getGPUEstimateForTransactions(
			List<Transaction> transactions) {
		Double time = 0.0;
		for (Transaction t : transactions) {
			time += t.getGPURunningTime();
		}
		return time;
	}

	private Map<Transaction, Map<Integer, Double>> computeScores(
			List<Transaction> transactions) {
		Map<Transaction, Map<Integer, Double>> scores = new HashMap<>();
		Map<Transaction, List<Double>> transaction_metadata = new HashMap<>();
		double max_exec_ratio = 0;
		double max_pcie_overhead = 0;
		double min_exec_ratio = 1000;
		double min_pcie_overhead = 1000;
		for (Transaction transaction : transactions) {
			transaction_metadata.put(transaction, new ArrayList<Double>());
			List<Double> gpu_metadata = new ArrayList<Double>();
			double temp = transaction.getCPUExecutionTime()
					/ transaction.getGPURunningTime();
			if (temp > max_exec_ratio)
				max_exec_ratio = temp;
			if (temp < min_exec_ratio)
				min_exec_ratio = temp;
			for (int gpuID = 0; gpuID < Simulation.getInstance().getCluster()
					.getNumGPUSlots(); gpuID++) {
				Double dataToBeTransferred = 0.0;
				for (Tuple t : transaction.getReadSet()) {
					if (!Simulation.getInstance().getCluster()
							.doesGPUHaveLatestTupleVersion(gpuID, t,
									Simulation.getInstance().getCluster()
											.latestTupleVersion(t))) {
						dataToBeTransferred += t.getMemory();
					}
				}
				double temp2 = PCIeUtils
						.getHostToDeviceTransferTime(dataToBeTransferred)
						+ PCIeUtils.getDeviceToHostTransferTime(
								transaction.getOutputSize());
				if (temp2 > max_pcie_overhead)
					max_pcie_overhead = temp2;
				if (temp2 < min_pcie_overhead)
					min_pcie_overhead = temp2;
				gpu_metadata.add(temp2);
			}
			transaction_metadata.get(transaction).add(temp);
			for (Double gpu_overheads : gpu_metadata)
				transaction_metadata.get(transaction).add(gpu_overheads);
		}

		for (Transaction t : transactions) {
			scores.put(t, new HashMap<>());
			for (int i = 0; i < Simulation.getInstance().getCluster()
					.getNumGPUSlots(); i++) {
				scores.get(t).put(i,
						computeScore(transaction_metadata.get(t), i,
								max_exec_ratio, max_pcie_overhead,
								min_exec_ratio, min_pcie_overhead));
			}
		}
		return scores;
	}

	private Double computeScore(List<Double> transaction_metadata,
			Integer gpuID, Double maxExecRatio, Double maxPCIeOverhead,
			Double minExecRatio, Double minPCIeOverhead) {
		double exec_time_component;
		double pcie_component;
		if ((maxExecRatio - minExecRatio == 0.0)) {
			exec_time_component = 0;
		} else {
			exec_time_component = (transaction_metadata.get(0) - minExecRatio)
					/ (maxExecRatio - minExecRatio);
		}
		if ((maxPCIeOverhead - minPCIeOverhead == 0.0)) {
			pcie_component = 1;
		} else {
			pcie_component = (transaction_metadata.get(gpuID + 1)
					- minPCIeOverhead) / (maxPCIeOverhead - minPCIeOverhead);
		}

		return alpha * exec_time_component + (1 - alpha) * (1 - pcie_component);
	}
}

class TransactionOrderComparator implements Comparator<Transaction> {
	@Override
	public int compare(Transaction a, Transaction b) {
		int cmp = Double.compare(a.getAcceptStamp(), b.getAcceptStamp());
		if (cmp != 0) {
			return cmp;
		}
		// Tie break by transaction ID
		return a.getTransactionId() - b.getTransactionId();
	}
}

class TransactionExecutionContextComparator
		implements Comparator<TransactionExecutionContext> {
	@Override
	public int compare(TransactionExecutionContext o1,
			TransactionExecutionContext o2) {
		int cmp = Double.compare(o1.getTransaction().getAcceptStamp(),
				o2.getTransaction().getAcceptStamp());
		if (cmp != 0) {
			return cmp;
		}
		// Tie break by transaction ID
		return o1.getTransaction().getTransactionId()
				- o2.getTransaction().getTransactionId();
	}

}
