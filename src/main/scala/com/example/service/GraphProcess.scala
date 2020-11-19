package com.example.service

import java.sql.DriverManager
import java.util.Properties

import com.example.config.ConfigParser
import com.example.db.SqlStringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import com.example.utils.StringKeyUtils
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPair

object GraphProcess {

  def main(args: Array[String]) : Unit = {
    // 初始化spark context

    val spark = SparkSession.builder().
      appName(StringKeyUtils.STR_KEY_APPLICATION_NAME).
      master(StringKeyUtils.STR_KEY_SPARK_MODEL_LOCAL).getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    // 加载配置
    val config = ConfigParser.loadProperties(StringKeyUtils.STR_KEY_PROPERTIES_FILE_NAME)

    val driver = config.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_DRIVER)
    val sqlUrl = config.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_URL)
    val user = config.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_USER_NAME)
    val password = config.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_USER_PASSWORD)

    val properties = new Properties()
    properties.put(SqlStringUtils.STR_KEY_USER, user)
    properties.put(SqlStringUtils.STR_KEY_PASSWORD, password)
    properties.put(SqlStringUtils.STR_KEY_DRIVER, driver)

    // 读取数据库
    val df = spark.read.jdbc(sqlUrl, SqlStringUtils.STR_TAB_MIGRATION, properties)
    df.collect().foreach(println)
  }
}
