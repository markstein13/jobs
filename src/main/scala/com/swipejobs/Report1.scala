package com.swipejobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Write a scale, spark process to show the values of median bill rate, median pay rate and
 * distinct count of tickets grouped by job ticket state (both DISPATCHED and CANCELLED) based on:
 *
 * - Filtered workers data extracted through the filter-workers command.
 * - Filtered jobtickets data extracted through the filter-tickets command.
 */
object Report1 {

  def run(
    session: SparkSession,
    workerInput: WorkerInput,
    ticketInput: TicketInput,
    outputPath: OutputPath
  ): Unit = {
    val workers = session.read.parquet(workerInput.path.toString)
    val jobtickets = session.read.parquet(ticketInput.path.toString)

    val report = runWithData(workers, jobtickets)

    report.write.parquet(outputPath.path.toString)
  }

  /**
   * This allows callers to use DataFrames instead of Paths, desirable for testing and potentially
   * other applications/functions
   *
   * TODO This function should ideally return an Either[String, Unit] to capture errors, eg. schema
   *      validation.
   */
  def runWithData(workers: DataFrame, jobtickets: DataFrame): DataFrame = {
    // Join the workers and jobtickets data on worker id
    //
    // TODO need to share this with Report 2
    val ticketsJoined = workers
      .select("worker_gw_id")
      .join(jobtickets)
      .where("worker_gw_id = workerId")
      .drop("worker_gw_id")

    // For each worker create aggregations over job ticket states DISPATCHED and CANCELLED
    //  - distinct count of tickets
    //  - median bill rate
    //  - median pay rate
    // Note: The median calculation is an approximate for performance reasons
    ticketsJoined
      .groupBy("workerId")
      .pivot("jobTicketState", Array("DISPATCHED", "CANCELLED"))
      .agg(
        countDistinct("startTime").as("count"),
        expr("percentile_approx(billRate, 0.5) as medianBillRate"),
        expr("percentile_approx(payRate, 0.5) as medianPayRate")
      )
  }
}
