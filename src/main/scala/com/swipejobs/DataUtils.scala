package com.swipejobs

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataUtils {

  /**
   * A few datasets have a raw json field which contains data that needs to be accessed.
   * This function detects the schema of the json and replaces the field with a struct version.
   */
  def expandJson(
    session: SparkSession,
    fieldName: String,
    data: DataFrame
  ): DataFrame = {
    // extract the field raw json to a Dataset[String]
    val json = data.map((value: Row) => value.getAs[String](fieldName))(Encoders.STRING)

    // get spark to parse the json so we can get the schema out
    val jsonSchema = session.read.json(json).schema

    // convert the field json into a struct
    data.withColumn(fieldName, from_json(col(fieldName), jsonSchema))
  }

}
