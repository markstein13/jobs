package com.swipejobs

import java.time.LocalDate
import java.time.format.DateTimeParseException

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._

// Note: I am only using this to get access to a their version of the Either data type
//       (Disjunction). The problem with scala's version of Either in 2.11 is you can't
//       use it in a for comprehension. scala 2.12 has support for this in their Either
//       type but it seems to be more common for spark to be deployed against scala 2.11
//
import scalaz._, Scalaz._

sealed trait Command

object Command {
  case class FilterWorkers(
    workerInput: WorkerInput,
    output: OutputPath
  ) extends Command
  case class FilterTickets(
    ticketInput: TicketInput,
    output: OutputPath
  ) extends Command
  case class Report1(
    workerInput: WorkerInput,
    ticketInput: TicketInput,
    output: OutputPath
  ) extends Command
  case class Report2(
    workerInput: WorkerInput,
    ticketInput: TicketInput,
    stackRequestsInput: StackRequestsInput,
    output: OutputPath
  ) extends Command
  case class Report3(
    runDate: LocalDate,
    jobStackInput: JobStackInput,
    emptyStackInput: EmptyStackInput,
    output: OutputPath
  ) extends Command
}

/**
 * Cli application to run the different reports:
 *
 * - filter-workers: Filter the raw workers data. The output of this should be passed into
 *                   report1 and report2
 * - filter-tickets: Filter the raw jobtickets data. The output of this should be passed into
 *                   report1 and report2
 * - report1: Calculate median bill rate, median pay rate and distinct count of tickets grouped by
 *            job ticket state (both DISPATCHED and CANCELLED).
 *            Note: Use output of filter-workers and filter-tickets
 * - report2: Create a report of workers with time between first stack request for a shift and
 *            start time of that shift (startTime)
 *            Note: Use output of filter-workers and filter-tickets
 * - report3: Create a report on a given day of workers who each day only saw empty stacks,the next
 *            time, if any, they saw a job in their stack and the next day they requested a stack
 *            on.
 */
object App {

  def main(args: Array[String]): Unit = {
    // TODO Use a proper cli lib like https://github.com/markhibberd/pirate
    val command = args.toList match {
      case "filter-workers" :: rawWorkerPath :: rawOutput :: Nil =>
        for {
          workerInput <- parsePath(rawWorkerPath).map(WorkerInput)
          output <- parsePath(rawOutput).map(OutputPath)
        } yield Command.FilterWorkers(workerInput, output)
      case "filter-tickets" :: rawTicketPath :: rawOutput :: Nil =>
        for {
          ticketInput <- parsePath(rawTicketPath).map(TicketInput)
          output <- parsePath(rawOutput).map(OutputPath)
        } yield Command.FilterTickets(ticketInput, output)
      case "report1" :: rawWorkerPath :: rawTicketPath :: rawOutput :: Nil =>
        for {
          workerInput <- parsePath(rawWorkerPath).map(WorkerInput)
          ticketInput <- parsePath(rawTicketPath).map(TicketInput)
          output <- parsePath(rawOutput).map(OutputPath)
        } yield Command.Report1(workerInput, ticketInput, output)
      case "report2" :: rawWorkerPath :: rawTicketPath :: rawStackRequestsPath :: rawOutput :: Nil =>
        for {
          workerInput <- parsePath(rawWorkerPath).map(WorkerInput)
          ticketInput <- parsePath(rawTicketPath).map(TicketInput)
          stackRequestsInput <- parsePath(rawStackRequestsPath).map(StackRequestsInput)
          output <- parsePath(rawOutput).map(OutputPath)
        } yield Command.Report2(workerInput, ticketInput, stackRequestsInput, output)
      case "report3" :: rawDate :: rawJobStackPath :: rawEmptyStackPath :: rawOutput :: Nil =>
        for {
          date <- parseDate(rawDate)
          jobStackInput <- parsePath(rawJobStackPath).map(JobStackInput)
          emptyStackInput <- parsePath(rawEmptyStackPath).map(EmptyStackInput)
          output <- parsePath(rawOutput).map(OutputPath)
        } yield Command.Report3(date, jobStackInput, emptyStackInput, output)
      case _ =>
        s"Unknown command '${args.mkString(" ")}'".left
        // TODO add help message
    }
    command.map(run) match {
      case -\/(message) =>
        sys.error(s"Process failed: $message")
      case \/-(()) =>
        ()
    }
  }

  def run(command: Command): Unit = {
    val session = SparkSession.builder.getOrCreate
    command match {
      case Command.FilterWorkers(workerInput, output) =>
        FilterWorkers.run(session, workerInput, output)
      case Command.FilterTickets(ticketInput, output) =>
        FilterTickets.run(session, ticketInput, output)
      case Command.Report1(workerInput, ticketInput, output) =>
        Report1.run(session, workerInput, ticketInput, output)
      case Command.Report2(workerInput, ticketInput, stackRequestsInput, output) =>
        Report2.run(session, workerInput, ticketInput, stackRequestsInput, output)
      case Command.Report3(runDate, jobStackInput, emptyStackInput, output) =>
        Report3.run(session, runDate, jobStackInput, emptyStackInput, output)
    }
  }

  def parseDate(raw: String): Disjunction[String, LocalDate] =
    try {
      LocalDate.parse(raw).right
    } catch {
      case e: DateTimeParseException =>
        s"Can not parse date '$raw' - ${e.getMessage}".left
    }

  def parsePath(raw: String): Disjunction[String, Path] =
    try {
      new Path(raw).right
    } catch {
      case e: IllegalArgumentException =>
        s"Invalid path '$raw' - ${e.getMessage}".left
    }
}
