package com.intel.distml.platform;

import java.io.Serializable;

public class TrainingContext implements Serializable {

    public long totalSampleCount = 0;
    public int progressStepSize = 0;

    public int miniBatchSize = 1;
    public int iteration = 1;

    public int psCount = 1;
    public int workerCount = 1;

    public int currentIter = 0;

    public TrainingContext() {

    }

    public TrainingContext psCount(int psCount) {
        this.psCount = psCount;
        return this;
    }

    public TrainingContext workerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }

    public TrainingContext miniBatchSize(int miniBatchSize) {
        this.miniBatchSize = miniBatchSize;
        return this;
    }

    public TrainingContext iteration(int iteration) {
        this.iteration = iteration;
        return this;
    }
}