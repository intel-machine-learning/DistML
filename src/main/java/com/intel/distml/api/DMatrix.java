package com.intel.distml.api;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

import java.util.List;

public class DMatrix extends Matrix {

//	public static final int TYPE_SAMPLE = 0;
//	public static final int TYPE_PARAM = 1;
//	public static final int TYPE_DATA = 2;
//	public static final int TYPE_ERROR = 3;
//	public static final int TYPE_UPDATE = 4;
//	public static final int TYPE_DELTA = 5;

	public static final int FLAG_PARAM 	= 1;
	public static final int FLAG_UPDATE	= 2;
	public static final int FLAG_ON_SERVER = 4;
	public static final int FLAG_ON_WORKER = 8;

	public static final int PARTITION_STRATEGY_LINEAR = 0;
	public static final int PARTITION_STRATEGY_HASH = 1;

	protected PartitionInfo workerPartitions, serverPartitions;
	protected int partitionStrategy;

	public Matrix localCache;

	public int flags;
	protected KeyRange rowKeys;

	public DMatrix(long rows) {
		this(FLAG_ON_WORKER, rows);
	}

	public DMatrix(int flags, long rows) {
		this.flags = flags;
		this.partitionStrategy = PARTITION_STRATEGY_LINEAR;

		rowKeys = new KeyRange(0, rows-1);
	}

	public boolean hasFlag(int flag) {
		System.out.println("check flag: " + flags + ", " + flag + ", " + rowKeys.size());
		return (flags & flag) > 0;
	}

	public KeyCollection getRowKeys() {
		return rowKeys;
	}

	public KeyCollection getColKeys() {
		return KeyRange.Single;
	}

	public PartitionInfo workerPartitions() {
		return workerPartitions;
	}

	public PartitionInfo serverPartitions() {
		return serverPartitions;
	}

	public void setPartitionStrategy(int strategy) {
		if ((strategy != PARTITION_STRATEGY_LINEAR) && (strategy != PARTITION_STRATEGY_HASH)) {
			throw new IllegalArgumentException("partiton strategy must be SPLIT or HASH.");
		}

		partitionStrategy = strategy;
	}

	void partition(int serverNum) {
		System.out.println("partitioning: servers=" + serverNum + ", strategy=" + partitionStrategy + ", rows=" + rowKeys.size());

		KeyCollection[] keySets;
		if (partitionStrategy == PARTITION_STRATEGY_LINEAR) {
			keySets = rowKeys.linearSplit(serverNum);
		}
		else {
			keySets = rowKeys.hashSplit(serverNum);
		}

		PartitionInfo info = new PartitionInfo(PartitionInfo.Type.PARTITIONED);

		for (int i = 0; i < serverNum; i++) {
			Partition p = new Partition();
			p.index = i;
			p.keys = keySets[i];
			info.addPartition(p);
		}

		serverPartitions = info;
	}

	public void setLocalCache(Matrix matrix) {
		this.localCache = matrix;
	}

	/**
	 * For a worker whose index is workerIndex, this method returns needed rows to fetch
	 * from parameter server whose index is workerIndex.
	 *
	 * @param wp
	 * @param sp
	 * @return
	 */
	public KeyCollection intersectRows(Partition wp, Partition sp) {
		if (sp == null) {
			if (wp == null) {
				return getRowKeys();
			}

			return wp.keys;
		}
		else {
			if (wp == null) {
				return sp.keys;
			}

			return sp.intersect(wp);
		}
	}

	public Matrix subMatrix(KeyCollection rowKeys, KeyCollection colKeys) {
		if (localCache == null) {
			throw new RuntimeException("No cache data found");
		}

		return localCache.subMatrix(rowKeys, colKeys);
	}

	@Override
	public boolean mergeMatrices(List<Matrix> matrices) {
		if (localCache == null) {
			throw new RuntimeException("No cache data found");
		}

		return localCache.mergeMatrices(matrices);
	}

	public boolean mergeMatrix(Matrix matrix) {
		if (localCache == null) {
			throw new RuntimeException("No cache data found");
		}

		return localCache.mergeMatrix(matrix);
	}

	public void initOnServer(int psIndex, KeyCollection keys) {
	}

	public void initOnWorker(int workerIndex, KeyCollection keys) {

	}

	public void mergeUpdate(int serverIndex, Matrix update) {

	}
}
