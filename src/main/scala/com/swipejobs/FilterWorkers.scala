package com.swipejobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * This is a separate job used to filter workers by max job distance is less than 30 miles
 */
object FilterWorkers {

  def run(
    session: SparkSession,
    workerInput: WorkerInput,
    outputPath: OutputPath
  ): Unit = {
    val workers = session.read.parquet(workerInput.path.toString)

    val filteredWorkers = runWithData(workers)

    filteredWorkers.write.parquet(outputPath.path.toString)
  }

  /**
   * This allows callers to use DataFrames instead of Paths, desirable for testing and potentially
   * other applications/functions
   *
   * TODO This function should ideally return an Either[String, Unit] to capture errors, eg. schema
   *      validation.
   */
  def runWithData(workers: DataFrame): DataFrame = {
    workers.where("maxJobDistance < 30.0")
  }
}
