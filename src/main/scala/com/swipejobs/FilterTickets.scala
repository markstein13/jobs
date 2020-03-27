package com.swipejobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * This is a separate job used to filter tickets by assigned by swipejobs and first shifts
 * (earliest createdAt) only
 */
object FilterTickets {

  def run(
    session: SparkSession,
    ticketInput: TicketInput,
    outputPath: OutputPath
  ): Unit = {
    val jobtickets = session.read.parquet(ticketInput.path.toString)

    val filteredTickets = runWithData(jobtickets)

    filteredTickets.write.parquet(outputPath.path.toString)
  }

  /**
   * This allows callers to use DataFrames instead of Paths, desirable for testing and potentially
   * other applications/functions
   *
   * TODO This function should ideally return an Either[String, Unit] to capture errors, eg. schema
   *      validation.
   */
  def runWithData(jobtickets: DataFrame): DataFrame = {
    // Need a window function to select the first shift in the jobtickets data.
    val ticketWindow = Window.partitionBy("ticketId").orderBy("createdAt")

    // Filter jobtickets by assigned by swipejobs and first shifts (earliest createdAt) only
    jobtickets
      .where("assignedBySwipejobs = true")
      .withColumn("row", row_number().over(ticketWindow))
      .where("row = 1")
      .drop("row")
  }
}
