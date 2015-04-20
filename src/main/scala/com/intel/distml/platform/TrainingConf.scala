package com.intel.distml.platform

/**
 * Training process:
 * epoch 0 .... n
 *    shuffle samples
 *    sampling
 *    iteration  0 ...... n
 *      [optional] shuffle
 *      get batch samples
 *      fetch parameters
 *      sample 0 ...... batch_size
 *          compute    -- in workers
 *      upload delta
 *
 */
class TrainingConf (
) extends Serializable {

  var totalSampleCount: Long = 0;
  var progressStepSize: Int = 0;

  var miniBatchSize: Int = 1
  var iteration: Int = 1

  var psCount: Int = 1
  var groupCount: Int = 1
  var groupSize: Int = 1

  def psCount(psCount: Int): TrainingConf = {
    this.psCount = psCount
    this
  }

  def groupCount(groupCount: Int): TrainingConf = {
    this.groupCount = groupCount
    this
  }

  def groupSize(groupSize: Int): TrainingConf = {
    this.groupSize = groupSize
    this
  }

  def miniBatchSize(miniBatchSize: Int): TrainingConf = {
    this.miniBatchSize = miniBatchSize
    this
  }

  def iteration(iteration: Int): TrainingConf = {
    this.iteration = iteration
    this
  }
}