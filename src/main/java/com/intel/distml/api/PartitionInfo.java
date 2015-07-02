package com.intel.distml.api;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Created by yunlong on 12/13/14.
 */
public class PartitionInfo implements Serializable {

	public enum Type {
        COPIED,
        PARTITIONED,
        EXCLUSIVE
	}
	
    public Type type;
    //public int partitionNum; Never used now.
    public int exclusiveIndex;
    public LinkedList<Partition> partitions;
    
    public PartitionInfo(Type type) {
    	this.type = type;
    	if (type == Type.PARTITIONED) {
    		partitions = new LinkedList<Partition>();
    	}
    }

    public void addPartition(Partition p) {
        partitions.add(p);
    }
    
    public Partition getPartition(int partitionIndex) {
    	Partition partition = partitions.get(partitionIndex);
    	return partition;
    }
    
    public int size() {
    	return partitions.size();
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        String s = "" + type;
        // add more information here

        return s;
    }
}
