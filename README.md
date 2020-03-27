Jobs Reports
------------

## Brief

Swipejobs is all about matching Jobs to Workers. Your challenge is to analyse the data provided and answer the questions below.

Please note the following:

* Workers receive "Stack Requests" (job matches) on their app.
* Workers dispatch to Jobs through the app
* A Worker has one or more job tickets associated with them

Using parquet files provided in the s3 bucket 

* bucket name : test.datascience.swipejobs.com
* This contains the following directories of parquet data
  * emptystacks
  * jobstacks
  * jobtickets
  * stackrequests
  * worker

Part 1 : Write a scale, spark process to show the values of median bill rate, median pay rate and
distinct count of tickets grouped by job ticket state (both DISPATCHED and CANCELLED) based on:

1. The latest backup of Worker collection that is filtered by max job distance is less than 30
   miles (in the worker directory)
2. All Ticket backups that are filtered by assigned by swipejobs and first shifts (earliest
   createdAt) only

Part 2. Based on:

1. Raw data collected from part 1
2. Stack requests occurred between 2017-11-01 and 2017-11-06 that are filtered by first shifts only
   (earliest event_time)
3. Empty Stacks received by workers

Show the following values on a single output:

* 2.1 ticketId, workerId, status, subOrderId, shiftId, assignedBySwipejobs, maxJobDistance,
  milesToTravel, signUpDate, jobTicketState, billRate, payRate, startTime
* 2.2 Time between first stack request for a shift and start time of that shift (startTime)
* 2.3 List of workers who each day only saw empty stacks,the next time, if any, they saw a job in
  their stack and the next day they requested a stack on. This should be saved as a daily parquet
  file containing workerId, nextStackRequest, nextTimeTheySawAJob

Please note that, there may be some data loss on stack requests collection. The values of 2.1 should be shown even though there is no corresponding stack request for that shift.

All 3 of the reports should be saved to s3 in parquet format with the given path:

`test.datascience.swipejobs.com/results/submitter_name<submitter_name>/_part/report_date<report_date>/report.csv</report_date></submitter_name>`

Example:

`submitter_name_russell_aaronson/_part_1/report_date_2017-11-10/report.csv`

## Extra Notes

The test directory contains 2 different types of data source, The full business flow and usage for
both is detailed below

1. daily logs of events which happen (segregated by event type), this is captured from the
   application event stream and timestamped and windowed by the time of the event.
  1. stackrequests
  2. emptystack
  3. jobstack
2. database backups of certain collections from the microservices which store the state of
   certain things
  1. workers
  2. jobtickets

The detailed Business flow is described here:

* A worker requests a set of matches (or a "Jobstack" from the system) - occurrences of this
  event are captured in the stackrequests directory
* The system then generates a realtime jobstack for that user based on available jobs, this stack
  is recorded in one of two events...
  * If there are matching jobs in the stack this is captured in a "jobstack" event, along with
    some information about jobs shown to the worker
  * If there are no matching jobs in the stack this is captured in a "emptystack" event and
    stored in the emptystack parquet structure
* If the worker sees a job (from a jobstack) and accepts it then a jobticket is created in the
  jobticket database  (This table tracks the jobticket lifecycle including the outcome, in terms
  of hours worked and ratings) this is held in the "jobticket", parquet structure  (NOTE the
  fields in this database are immutable and the backup is the latest state of the ticket)

The "worker" parquet directory contains daily backups of the worker information from the worker
database, this data is volatile so it contains daily backups at the end of each day.

## Tradeoffs

1. Because median is finding the middle number in a sorted group, it could get expensive depending
on the size of the group. Because of this there are "cheap" functions in spark which give an
approximate median. I think this is a good tradeoff because the value will probably be fed into an
ML model so accuracy isn't strickly required.

## Spark shell solution (first pass)

```scala
// Part 1

val workersFiltered = workers.where("maxJobDistance < 30.0")

val ticketWindow = Window.partitionBy("ticketId").orderBy("createdAt")
val ticketsFiltered = jobtickets.where("assignedBySwipejobs = true").withColumn("row", row_number().over(ticketWindow)).where("row = 1").drop("row")

val ticketsJoined = workersFiltered.select("worker_gw_id").join(ticketsFiltered).where("worker_gw_id = workerId").drop("worker_gw_id")

ticketsJoined.groupBy("workerId").pivot("jobTicketState", Array("DISPATCHED", "CANCELLED")).agg(countDistinct("startTime").as("count"), expr("percentile_approx(billRate, 0.5) as medianBillRate"), expr("percentile_approx(payRate, 0.5) as medianPayRate"))

// Part 2

// 2.2 To find the time between the first stack request for a shift and the start time of that
// shift, we need to:
// - Turn stackrequests data into a table of shiftId, event_time
// - Filter it down to earliest event_time for each shiftId
// - Join the filtered table to the ticketsFiltered table from Part 1
// - Calculate time difference between event_time and startTime

// extract the "body" raw json to a Dataset[String]
val bodyJson = stackrequests.map((value: Row) => value.getAs[String]("body"))(Encoders.STRING)

// get spark to parse the json so we can get the schema out
val bodySchema = spark.read.json(bodyJson).schema

// convert the "body" json into a struct
val stackrequestsWithBody = stackrequests.withColumn("body", from_json($"body", bodySchema))

// create shiftId, event_time data
val shifts = stackrequestsWithBody.selectExpr("event_time", "explode(body.payload.items) as item").selectExpr("event_time", "explode(item.shifts) as shift").select("shift.shiftId", "event_time")

// filter shifts
val shiftsWindow = Window.partitionBy("shiftId").orderBy("event_time")
val earliestShifts = shifts.withColumn("row", row_number().over(shiftsWindow)).where("row = 1").drop("row")

// join to the ticketsFiltered dataframe from part 1
val shiftsWithTickets = ticketsFiltered.join(earliestShifts, "shiftId")

// Calculate time diff
shiftsWithTickets.withColumn("timeToStartShift", unix_timestamp(col("startTime")) - unix_timestamp(col("event_time")))

// 2.3 Per day find a list of workers who only had empty stack requests along with the next day
// they made a stack request and the next time they saw a job

val runDate = "2017-11-05"

// load and filter input >= runDate

// get json schema of body
val jobstacksBodySchema = spark.read.json(jobstacks.map((value: Row) => value.getAs[String]("body"))(Encoders.STRING)).schema
val emptystacksBodySchema = spark.read.json(emptystacks.map((value: Row) => value.getAs[String]("body"))(Encoders.STRING)).schema
// expand body json out
val jobstacksWithBody = jobstacks.withColumn("body", from_json($"body", jobstacksBodySchema))
val emptystacksWithBody = emptystacks.withColumn("body", from_json($"body", emptystacksBodySchema))

// create clean versions of jobstacks and emptystacks
val jobstacksClean = jobstacksWithBody.selectExpr("body.payload as worker_gw_id", "event_time", "requestid")
val emptystacksClean = emptystacksWithBody.selectExpr("body.payload as worker_gw_id", "event_time", "requestid")

// union the two
val allRequests = jobstacksClean.withColumn("request_type", lit("jobstack")).union(emptystacksClean.withColumn("request_type", lit("emptystack")))

// List of workers per day with only emptystack requests
// Note: When using all data, use event_date instead of creating event_day
val workersWithEmptystacks = allRequests.withColumn("event_day", expr("to_date(event_time)")).groupBy("worker_gw_id", "event_day").pivot("request_type").agg(max("event_time").as("latest_event_time"), count("*").as("count")).select("worker_gw_id", "event_day", "emptystack_latest_event_time", "emptystack_count", "jobstack_count").where("emptystack_count > 0 AND jobstack_count = 0").select("worker_gw_id", "event_day", "emptystack_latest_event_time")

// Note: allRequests contains data from runDate, we need to filter that day out specifically
val nextStackrequests = allRequests.withColumn("event_day", expr("to_date(event_time)")).where(s"event_day > date('$runDate')").groupBy("worker_gw_id").agg(min("event_day").as("nextStackRequest"))
val nextSawJob = jobstacksClean.withColumn("event_day", expr("to_date(event_time)")).where(s"event_day > date('$runDate')").groupBy("worker_gw_id").agg(min("event_time").as("nextTimeTheySawAJob"))

// Join the above two
val j1 = workersWithEmptystacks.joinWith(nextStackrequests.withColumnRenamed("worker_gw_id", "workerId"), expr("worker_gw_id = workerId"), "left_outer").select("_1.worker_gw_id", "_2.nextStackRequest")
val workersWithEmptystacksJoined = j1.joinWith(nextSawJob.withColumnRenamed("worker_gw_id", "workerId"), expr("worker_gw_id = workerId"), "left_outer").select("_1.worker_gw_id", "_1.nextStackRequest", "_2.nextTimeTheySawAJob")
```

## Application solution

This approach moves the above steps into a spark application, using a cli interface to run each
step. The reason i went with this approach:
* Can create a workflow with any external tool (oozie, airflow, etc) which conntects the different
  jobs together.
* Can manually run each step if required
* Easier to test

The different cli commands:

* `filter-workers`: This applies the filtering rules specified in Part 1
  * The reason this is a separate command is because its required input for two other commands
* `filter-tickets`: This applies the filtering rules specified in Part 1
  * The reason this is a separate command is because its required input for two other commands
* `report1`: This is the solution to part 1
* `report2`: This is the solution to part 2.1 and 2.2
* `report3`: This is the solution to part 2.2

Examples:

```

```

### Note: Lack of tests

Due to the amount of time spent understanding the data and working on the solution, i didn't have
enough time to write tests. Normally i would write extensive unit and integration tests. This is
what i would have done with more time:

* Unit tests
  * Traditional example based unit testing of functions
  * Property based tests to get greater coverage, specifically https://github.com/hedgehogqa/scala-hedgehog
* Integration tests
  * This would run the application on a hadoop cluster with a relatively small amount of data

## CI/CD

CI is done through both github and AWS (just trying them both out).

I tried using https://aws.amazon.com/blogs/big-data/implement-continuous-integration-and-delivery-of-apache-spark-applications-using-aws/
as a guide to setup a pipeline for both CI and CD, but i ran out of time and didn't get it fully
working.

To launch a CloudFormation stack:

```
aws cloudformation create-stack --template-body "$(cat emrSparkPipeline.yaml)" --stack-name emr-spark-pipeline --capabilities CAPABILITY_NAMED_IAM
```
