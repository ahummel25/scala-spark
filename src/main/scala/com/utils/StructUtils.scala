package com.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StructUtils {
  def getMCEFSchemaStruct: StructType = {
     StructType(
      Seq(
        StructField("RECORDID", StringType, nullable = false),
        StructField("RXCLAIMNBR", StringType, nullable = false),
        StructField("CLMSEQNBR", StringType, nullable = false),
        StructField("CLAIMSTS", StringType, nullable = false)
      )
    )
  }
}
