package com.swipejobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Create a shifts report that also contains time between first stack request for a shift and
 * start time of that shift (startTime)
 *
 * Input:
 *  - Filtered jobtickets data extracted through the filter-tickets command.
 *  - stackrequests
 */
object Report2 {

  def run(
    session: SparkSession,
    workerInput: WorkerInput,
    ticketInput: TicketInput,
    stackRequestsInput: StackRequestsInput,
    outputPath: OutputPath
  ): Unit = {
    val workers = session.read.parquet(workerInput.path.toString)
    val jobTickets = session.read.parquet(ticketInput.path.toString)
    val stackRequests = DataUtils.expandJson(
      session,
      "body",
      session.read.parquet(stackRequestsInput.path.toString)
    )

    val report = runWithData(workers, jobTickets, stackRequests)

    report.write.parquet(outputPath.path.toString)
  }

  /**
   * This allows callers to use DataFrames instead of Paths, desirable for testing and potentially
   * other applications/functions
   *
   * TODO This function should ideally return an Either[String, Unit] to capture errors, eg. schema
   *      validation.
   */
  def runWithData(workers: DataFrame, jobTickets: DataFrame, stackRequests: DataFrame): DataFrame = {
    // 2.2 To find the time between the first stack request for a shift and the start time of that
    // shift, we need to:
    // - Turn stackrequests data into a table of shiftId, event_time
    // - Filter it down to earliest event_time for each shiftId
    // - Join the filtered table to the ticketsFiltered table from Part 1
    // - Calculate time difference between event_time and startTime

    // create shiftId, event_time data
    val shifts = stackRequests
      .selectExpr("event_time", "explode(body.payload.items) as item")
      .selectExpr("event_time", "explode(item.shifts) as shift")
      .select("shift.shiftId", "event_time")

    // filter shifts
    val shiftsWindow = Window.partitionBy("shiftId").orderBy("event_time")
    val earliestShifts = shifts
      .withColumn("row", row_number().over(shiftsWindow))
      .where("row = 1")
      .drop("row")

    // join to the ticketsFiltered dataframe from part 1 and calculate time diff
    val shiftsWithTickets = jobTickets
      .join(earliestShifts, "shiftId")
      .withColumn(
        "timeToStartShift",
        unix_timestamp(col("startTime")) - unix_timestamp(col("event_time"))
      )

    workers
      .withColumnRenamed("worker_gw_id", "workerId")
      .join(shiftsWithTickets, "workerId")
      .select("ticketId", "workerId", "status", "subOrderId", "shiftId", "assignedBySwipejobs",
        "maxJobDistance", "milesToTravel", "signUpDate", "jobTicketState", "billRate", "payRate",
        "startTime", "timeToStartShift")
  }
}
