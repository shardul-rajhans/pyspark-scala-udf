package com.example.udf
package application

import sdk.UDFBuilder

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScalaUdfRunner extends App {

  def main(): Unit = {
    println("Welcome to Scala UDF Runner")

    // Spark Session
    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    // Input Dataframe
    val input: DataFrame = session.read
      .option("header", value = true)
      .option("delimiter", value = "|")
      .csv(path = "src/main/resources/input_data.csv")


    // UDF for Concatenation of Addressing Fields
    val addressUDF: UserDefinedFunction = new UDFBuilder()
      .forAddress()

    // UDF for splitting into Addressing Fields
    val reverseAddressUDF: UserDefinedFunction = new UDFBuilder()
      .forReverseAddress()

    println("Running Scala UDFs...")

    val addressUdfResult: DataFrame = input.withColumn("concat_result",
      addressUDF(map(
        lit("_Address_"), col("Address"),
        lit("_Country_"), col("Country"),
        lit("_PostalCode_"), col("PostalCode")
      )))

    val reverseAddressUdfResult: DataFrame = input.withColumn("split_result",
      reverseAddressUDF(col("CompleteAddress")))

    addressUdfResult.drop("CompleteAddress").show(truncate = false)
    reverseAddressUdfResult.drop("Address", "Country", "PostalCode")
      .show(truncate = false)
  }

  main()
}
