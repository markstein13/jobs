package com.swipejobs

import java.time.LocalDate

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * 2.3 List of workers who each day only saw empty stacks,the next time, if any, they saw a job in
 * their stack and the next day they requested a stack on. This should be saved as a daily parquet
 * file containing workerId, nextStackRequest, nextTimeTheySawAJob
 *
 * Input:
 *  - runDate
 *  - jobstacks
 *  - emptystacks
 */
object Report3 {

  def run(
    session: SparkSession,
    runDate: LocalDate,
    jobStackInput: JobStackInput,
    emptyStackInput: EmptyStackInput,
    outputPath: OutputPath
  ): Unit = {
    val jobStacks = cleanStackData(
      DataUtils.expandJson(session, "body", session.read.parquet(jobStackInput.path.toString))
    )
    val emptyStacks = cleanStackData(
      DataUtils.expandJson(session, "body", session.read.parquet(emptyStackInput.path.toString))
    )

    val report = runWithData(runDate, jobStacks, emptyStacks)

    report.write.parquet(outputPath.path.toString)
  }

  def cleanStackData(stackData: DataFrame): DataFrame =
    stackData.selectExpr("body.payload as worker_gw_id", "event_time", "requestid")

  /**
   * This allows callers to use DataFrames instead of Paths, desirable for testing and potentially
   * other applications/functions
   *
   * TODO This function should ideally return an Either[String, Unit] to capture errors, eg. schema
   *      validation.
   */
  def runWithData(runDate: LocalDate, jobStacks: DataFrame, emptyStacks: DataFrame): DataFrame = {
    val filteredJobStacks = jobStacks.where(s"event_time > date('$runDate')")
    val filteredEmptyStacks = emptyStacks.where(s"event_time > date('$runDate')")

    // union the two to get all the requests
    val allRequests = filteredJobStacks
      .withColumn("request_type", lit("jobstack"))
      .union(filteredEmptyStacks.withColumn("request_type", lit("emptystack")))

    // List of workers per day with only emptystack requests
    // Note: When using all data, use event_date instead of creating event_day
    val workersWithEmptyStacks = allRequests
      .withColumn("event_day", expr("to_date(event_time)"))
      .groupBy("worker_gw_id", "event_day")
      .pivot("request_type")
      .agg(max("event_time").as("latest_event_time"), count("*").as("count"))
      .select("worker_gw_id", "event_day", "emptystack_latest_event_time", "emptystack_count",
        "jobstack_count")
      .where("emptystack_count > 0 AND jobstack_count = 0")
      .select("worker_gw_id", "event_day", "emptystack_latest_event_time")

    // Note: allRequests contains data on runDate, we need to filter that day out specifically
    val nextStackRequests = allRequests
      .withColumn("event_day", expr("to_date(event_time)"))
      .where(s"event_day > date('$runDate')")
      .groupBy("worker_gw_id")
      .agg(min("event_day").as("nextStackRequest"))

    val nextSawJob = filteredJobStacks
      .withColumn("event_day", expr("to_date(event_time)"))
      .where(s"event_day > date('$runDate')")
      .groupBy("worker_gw_id")
      .agg(min("event_time").as("nextTimeTheySawAJob"))

    // Join the above two
    workersWithEmptyStacks
      .joinWith(
        nextStackRequests.withColumnRenamed("worker_gw_id", "workerId"),
        expr("worker_gw_id = workerId"),
        "left_outer"
      )
      .select("_1.worker_gw_id", "_2.nextStackRequest")
      .joinWith(
        nextSawJob.withColumnRenamed("worker_gw_id", "workerId"),
        expr("worker_gw_id = workerId"),
        "left_outer"
      )
      .select("_1.worker_gw_id", "_1.nextStackRequest", "_2.nextTimeTheySawAJob")

    
  }

}
