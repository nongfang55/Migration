package com.example.db

import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType}

object TableSchemaHelper {
  // 最后以 Dataframe的形式写入数据库，用于保存需要写入数据库的schema

  val SCHEMA_PROVINCE_MIGRATION_PAGE_RANK: StructType = StructType{
    List(
      StructField("datetime", StringType, nullable = false),
      StructField("locationId", StringType, nullable = false),
      StructField("direction", BooleanType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    )
  }

  val SCHEMA_PROVINCE_MIGRATION_TREND: StructType = StructType{
    List(
      StructField("datetime", StringType, nullable = false),
      StructField("locationId", StringType, nullable = false),
      StructField("direction", BooleanType, nullable = false),
      StructField("ratio", DoubleType, nullable = true)
    )
  }

  val SCHEMA_PROVINCE_MIGRATION_DETAIL: StructType = StructType{
    List(
      StructField("datetime", StringType, nullable = false),
      StructField("sourceLocationId", StringType, nullable = false),
      StructField("destinationLocationId", StringType, nullable = false),
      StructField("direction", BooleanType, nullable = false),
      StructField("ratio", DoubleType, nullable = true)
    )
  }

  val SCHEMA_PROVINCE_RISK_LEVEL: StructType = StructType{
    List(
      StructField("datetime", StringType, nullable = false),
      StructField("locationId", StringType, nullable = false),
      StructField("direction", BooleanType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    )
  }

  val SCHEMA_PROVINCE_MIGRATION_PERSONAL_RANK: StructType = StructType{
    List(
      StructField("datetime", StringType, nullable = false),
      StructField("sourceLocationId", StringType, nullable = false),
      StructField("destinationLocationId", StringType, nullable = false),
      StructField("direction", BooleanType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    )
  }

}
