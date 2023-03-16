package com.example.udf
package sdk

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


// Builder class to create UDF Functions
// User can provide and create complex UDFs here according to requirement
class UDFBuilder {

  // Returns concatenated address from Addressing Fields
  def forAddress(): UserDefinedFunction = {
    def completeAddressProvider: Map[String, String] => String = {
      addressFields => s"${addressFields.getOrElse("_Address_", "")},${addressFields.getOrElse("_Country_", "")},${addressFields.getOrElse("_PostalCode_", "")}"
    }

    udf(completeAddressProvider)
  }

  // Splits concatenated address
  def forReverseAddress(): UserDefinedFunction = {
    def reverseAddressProvider: String => (String, String, String) = { str =>
      val seq = str.split(",").toSeq
      (seq.head, seq(1), seq(2))
    }

    udf(reverseAddressProvider)
  }
}