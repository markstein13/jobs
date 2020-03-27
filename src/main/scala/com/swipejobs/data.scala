package com.swipejobs

import org.apache.hadoop.fs.Path

//
// This is a file that holds lots of small data types
//

/** Type that represents the input path to the workers dataset */
case class WorkerInput(path: Path) extends AnyVal

/** Type that represents the input path to the jobtickets dataset */
case class TicketInput(path: Path) extends AnyVal

/** Type that represents the input path to the stackrequests dataset */
case class StackRequestsInput(path: Path) extends AnyVal

/** Type that represents the input path to the jobstacks dataset */
case class JobStackInput(path: Path) extends AnyVal

/** Type that represents the input path to the emptystacks dataset */
case class EmptyStackInput(path: Path) extends AnyVal

/** Type that represents an output path */
case class OutputPath(path: Path) extends AnyVal
