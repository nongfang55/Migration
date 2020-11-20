package com.example.service

import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.example.bean.Migration
import com.example.config.ConfigParser
import com.example.db.{SqlStringUtils, TableSchemaHelper}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import com.example.utils.{DateConvertHelper, ProvinceUtils, StringKeyUtils}
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPair
import org.apache.spark.graphx.{Edge, Graph}

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
    val resultDs = spark.read.jdbc(sqlUrl, SqlStringUtils.STR_TAB_MIGRATION, properties)

    // 通过时间对RDD进行过滤
    val date_t = "20200101"
    val dateStand = DateConvertHelper.convertStandTime(date_t)
    println("now filter date", date_t)

    val migrationRD = resultDs.rdd.map(f=>(f.get(0), f.get(1), f.get(2), f.get(3)))

    val filterRD = migrationRD.filter(f=>f._1.equals("20200101")).map(f=>f._4).
      map(f=>new String(f.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8))
//    println("after filter date", filterRD.count())

    //解析为Bean对象
    val beanRD = filterRD.map(f=>JSON.parseArray(f).get(0).asInstanceOf[JSONArray]).flatMap(f=>f.toArray())
      .map(f=>f.asInstanceOf[JSONObject].toString).map(f=>JSON.parseObject(f, classOf[Migration]))
    println("parser bean end", beanRD.count())


    val isIn = true // 定义方向
    //过滤全国信息  结果为（省份id,省流量）的二元组
    val countryMigrationRD = beanRD.filter(f=>ProvinceUtils.isCountry(f.sprovinceId)).
      filter(f=>ProvinceUtils.isIn(f.flag)==isIn)

    println("filter country rdd", countryMigrationRD.count())
    countryMigrationRD.foreach(println)

//    // 建立顶点RDD
    val vertexRDD = countryMigrationRD.map(f=>(f.mproId.toLong,f.province_name))
    val tupleCountryRDD = countryMigrationRD.map(f=>(f.mproId, f.value))

    // 计算边的权值
    // 中间计算结果为 (目标省份id, (源省份id, 比例))二元组
    val provinceMigrationRD = beanRD.filter(f=>(!ProvinceUtils.isCountry(f.sprovinceId))).
      filter(f=>ProvinceUtils.isIn(f.flag)==isIn).map(f=>(f.mproId, (f.sprovinceId, f.value)))
    println("filter province rdd", provinceMigrationRD)

    val tempJoinRDD = provinceMigrationRD.join(tupleCountryRDD)
//    tempJoinRDD.foreach(println)
    // join 结果实例  (120000,((540000,2.3),0.93))
    val edgeRDD = tempJoinRDD.map(f=>(f._2._1._1, f._1, f._2._2.toDouble * f._2._1._2.toDouble))
      .map(f=>Edge(f._1.toLong, f._2.toLong, f._3))
    edgeRDD.foreach(println)

    // 建图
    val graph = Graph(vertexRDD, edgeRDD)
    println("build graph success")

    // 计算pagerank
    val ranks = graph.pageRank(0.1).vertices
//    ranks.foreach(println)

    //pagerank 计算结果保存数据库
    val pageRankRDD = ranks.map(f=>Row(dateStand,f._1.toString, isIn, f._2))
    val pageRankDF = spark.createDataFrame(pageRankRDD,
      TableSchemaHelper.SCHEMA_PROVINCE_MIGRATION_PAGE_RANK)
    pageRankDF.show()
    pageRankDF.write.mode(SaveMode.Append).jdbc(sqlUrl, SqlStringUtils.STR_TAB_PROVINCE_MIGRATION_PAGE_RANK, properties)

    println("end")
  }
}
