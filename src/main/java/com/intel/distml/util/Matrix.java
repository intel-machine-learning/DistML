package com.intel.distml.util;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public abstract class Matrix implements Serializable {

//	protected KeyCollection rowKeys;
//	protected KeyCollection colKeys;

	public Matrix() {

	}

    public abstract KeyCollection getRowKeys();

	public abstract KeyCollection getColKeys();

//	public abstract T element(long row);
//	public abstract T element(long row, long col);

	public void show() {
		System.out.println("=== show() not implemented ===");
	}

	protected Matrix createEmptySubMatrix() {
		throw new RuntimeException("Not implemented.");
	}

	/**
	 * Follwoing cases need this method:
	 * 1. worker fetch some parameters from parameter server
	 * 		rowKeys specified
	 * 	    colKeys are null,means all colKey needed
	 * 	  server get sub-matrix as request and send back
	 *
	 * 2. worker fetch some errors from peer worker
	 * 		rowKeys are null, means all rowKey needed
	 * 		colKeys specified
	 *
	 *
	 * @param rowKeys
	 * @param colKeys
	 * @return
	 */
	public Matrix subMatrix(KeyCollection rowKeys, KeyCollection colKeys) {
		throw new RuntimeException("This method should be implemented by child classes if needed.");
	}

	/**
	 * Follwoing cases need this method:
	 * 1. worker fetch parameters from parameter servers
	 *    then merge these data in worker
	 *
	 * 2. worker fetch some errors from peer workers
	 * 	  then merge these data in worker
	 *
	 * @param matrices
	 */
	public boolean mergeMatrices(List<Matrix> matrices) {
		throw new RuntimeException("This method should be implemented by child classes if needed.");
	}

	public boolean mergeMatrix(Matrix matrix) {
		throw new RuntimeException("This method should be implemented by child classes if needed.");
	}

}
