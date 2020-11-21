package com.example.service

import java.util.Properties

import com.example.bean.Migration
import com.example.db.{SqlStringUtils, TableSchemaHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object DFStoreHelper {
  // 用于处理各种信息的保存

  def storeProvinceMigrationInformation(spark:SparkSession, countryMigrationRD:RDD[Migration],
                                        properties:Properties, dateStand:String, isIn:Boolean, sqlUrl:String): Unit = {
    // 获取对应的信息保存到 provinceMigrationTrend 表中
    val provinceMigrationTrendRDD = countryMigrationRD.map(f=>Row(dateStand, f.mproId, isIn, f.value.toDouble))
    val provinceMigrationTrendDF = spark.createDataFrame(provinceMigrationTrendRDD,
      TableSchemaHelper.SCHEMA_PROVINCE_MIGRATION_TREND)
    println("table:provinceMigrationTrend")
    provinceMigrationTrendDF.show()
    provinceMigrationTrendDF.write.mode(SaveMode.Append).jdbc(sqlUrl,
      SqlStringUtils.STR_TAB_PROVINCE_MIGRATION_TREND, properties)
  }

  def storeProvinceMigrationDetailInformation(spark:SparkSession, provinceMigrationDetailRD:RDD[Migration],
                                        properties:Properties, dateStand:String, isIn:Boolean, sqlUrl:String): Unit = {
    // 获取对应的信息保存到 provinceMigrationTrendDetail 表中
    val provinceMigrationDetailRowRD = provinceMigrationDetailRD.map(f=>Row(dateStand,f.sprovinceId, f.mproId,
      isIn, f.value.toDouble))
    val provinceMigrationDetailDF = spark.createDataFrame(provinceMigrationDetailRowRD,
      TableSchemaHelper.SCHEMA_PROVINCE_MIGRATION_DETAIL)
    println("table:provinceMigrationDetail")
    provinceMigrationDetailDF.show()
    provinceMigrationDetailDF.write.mode(SaveMode.Append).jdbc(sqlUrl,
      SqlStringUtils.STR_TAB_PROVINCE_MIGRATION_DETAIL, properties)
  }

}
