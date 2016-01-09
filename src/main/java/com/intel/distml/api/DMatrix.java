package com.intel.distml.api;

import com.intel.distml.util.DataDesc;
import com.intel.distml.util.DataStore;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;

import java.io.Serializable;

public class DMatrix implements Serializable {

	public static final int PARTITION_STRATEGY_LINEAR = 0;
	public static final int PARTITION_STRATEGY_HASH = 1;

	public KeyCollection[] partitions;

	protected KeyRange rowKeys;

	protected int partitionStrategy;

	protected String name;

	protected DataDesc format;

	protected DataStore store;

	public DMatrix(long rows) {
		this.partitionStrategy = PARTITION_STRATEGY_LINEAR;

		rowKeys = new KeyRange(0, rows-1);
	}

	public KeyCollection getRowKeys() {
		return rowKeys;
	}

	public KeyCollection getColKeys() {
		return KeyRange.Single;
	}

	public DataDesc getFormat() {
		return format;
	}

	public void setPartitionStrategy(int strategy) {
		if ((strategy != PARTITION_STRATEGY_LINEAR) && (strategy != PARTITION_STRATEGY_HASH)) {
			throw new IllegalArgumentException("partiton strategy must be SPLIT or HASH.");
		}

		partitionStrategy = strategy;
	}

	void partition(int serverNum) {

		KeyCollection[] keySets;
		if (partitionStrategy == PARTITION_STRATEGY_LINEAR) {
			keySets = rowKeys.linearSplit(serverNum);
		}
		else {
			keySets = rowKeys.hashSplit(serverNum);
		}

		partitions = keySets;
	}

	public DataStore getStore() {
		return store;
	}

	public DataStore setStore() {
		return store;
	}
}
