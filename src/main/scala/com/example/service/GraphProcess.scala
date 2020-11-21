package com.example.service

import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.example.bean.Migration
import com.example.config.ConfigParser
import com.example.db.{SqlStringUtils, TableSchemaHelper}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import com.example.utils.{DateConvertHelper, ProvinceUtils, StringKeyUtils}
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPair
import org.apache.spark.graphx.{Edge, Graph, GraphOps}

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

    // 读取数据库获得迁移信息
    val resultDs = spark.read.jdbc(sqlUrl, SqlStringUtils.STR_TAB_MIGRATION, properties)

    val migrationRD = resultDs.rdd.map(f=>(f.get(0), f.get(1), f.get(2), f.get(3)))
    // 读取数据库获得疫情信息
    val epidemicRawRDD = EpidemicDataLoader.loadEpidemicData(spark, sqlUrl, properties)

    val dateArray = DateConvertHelper.generateDateList(config.getProperty(ConfigParser.STR_KEY_CONFIG_PROCESS_START_DATE),
      config.getProperty(ConfigParser.STR_KEY_CONFIG_PROCESS_END_DATE))

    //按照日期的计算
    dateArray.foreach(date_t => {
      // 通过时间对RDD进行过滤
      println("now filter date", date_t)
      var isIn = true // 定义方向
      GraphProcess.processGraphBySingle(spark, date_t, isIn, migrationRD, epidemicRawRDD, properties, sqlUrl)
      isIn = false
      GraphProcess.processGraphBySingle(spark, date_t, isIn, migrationRD, epidemicRawRDD, properties, sqlUrl)
    })

    println("end")
  }

  def processGraphBySingle(spark:SparkSession, datetime:String, isIn:Boolean,
                           migrationRD:RDD[(Any, Any, Any, Any)], epidemicRawRDD:RDD[(String, String, Long)],
                           properties:Properties, sqlUrl:String): Unit = {
    // 把某日,某个方向的结果单独抽象出来
    val dateStand = DateConvertHelper.convertStandTime(datetime)

    val filterRD = migrationRD.filter(f=>f._1.equals(datetime)).map(f=>f._4).
      map(f=>new String(f.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8))
    println("after filter date", filterRD.count())

    //解析为Bean对象
    val beanRD = filterRD.map(f=>JSON.parseArray(f).get(0).asInstanceOf[JSONArray]).flatMap(f=>f.toArray())
      .map(f=>f.asInstanceOf[JSONObject].toString).map(f=>JSON.parseObject(f, classOf[Migration]))
    println("parser bean end", beanRD.count())

    //过滤全国信息  结果为（省份id,省流量）的二元组
    val countryMigrationRD = beanRD.filter(f=>ProvinceUtils.isCountry(f.sprovinceId)).
      filter(f=>ProvinceUtils.isIn(f.flag)==isIn)
    val provinceMigrationDetailRD = beanRD.filter(f=>(!ProvinceUtils.isCountry(f.sprovinceId))).
      filter(f=>ProvinceUtils.isIn(f.flag)==isIn)

    println("filter country rdd", countryMigrationRD.count())
    countryMigrationRD.foreach(println)

    // 保存全国各省的迁入或者迁出信息
    DFStoreHelper.storeProvinceMigrationInformation(spark, countryMigrationRD,
      properties, dateStand, isIn, sqlUrl)
    // 保存各省的迁入或者迁出信息明细
    DFStoreHelper.storeProvinceMigrationDetailInformation(spark, provinceMigrationDetailRD,
      properties, dateStand, isIn, sqlUrl)

    //获得省份感染的加权的权值
    val logEpidemicOffsetRDD = EpidemicDataLoader.NormalizeEpidemicRDByDate(spark, epidemicRawRDD,
      DateConvertHelper.convertStandTimeToRaw(datetime))

    // 建立顶点RDD
    val vertexRDD = countryMigrationRD.map(f=>(f.mproId.toLong,f.province_name)).cache()
    val tupleCountryRDD = countryMigrationRD.map(f=>(f.mproId, f.value))

    // 计算边的权值
    // 中间计算结果为 (目标省份id, (源省份id, 比例))二元组
    val provinceMigrationRD = provinceMigrationDetailRD.map(f=>(f.mproId, (f.sprovinceId, f.value)))
    println("filter province rdd", provinceMigrationRD)

    val tempJoinRDD = provinceMigrationRD.join(tupleCountryRDD)
    // join 结果实例  (120000,((540000,2.3),0.93))
    val edgeRDD = tempJoinRDD.map(f=>(f._2._1._1, f._1, f._2._2.toDouble * f._2._1._2.toDouble))
      .map(f=>Edge(f._1.toLong, f._2.toLong, f._3)).cache()
    edgeRDD.foreach(println)

    // 建图
    val graph = Graph(vertexRDD, edgeRDD)
    println("build graph success")

    // 计算pagerank
    val ranks = graph.ops.staticPageRank(5, 0.15).vertices
    //    ranks.foreach(println)

    //pagerank 计算结果保存数据库
    val pageRankRDD = ranks.map(f=>Row(dateStand,f._1.toString, isIn, f._2))
    val pageRankDF = spark.createDataFrame(pageRankRDD,
      TableSchemaHelper.SCHEMA_PROVINCE_MIGRATION_PAGE_RANK)
    pageRankDF.show()
    pageRankDF.write.mode(SaveMode.Append).jdbc(sqlUrl, SqlStringUtils.STR_TAB_PROVINCE_MIGRATION_PAGE_RANK, properties)

    // 计算加权的省份RDD
    val provinceRiskLevelRDD:RDD[Row] = pageRankRDD.map(f=>(f.getString(1),
      f.getDouble(3))).join(logEpidemicOffsetRDD).map(f=>Row(dateStand, f._1, isIn, f._2._1 * f._2._2))
    val provinceRiskLevelDF = spark.createDataFrame(provinceRiskLevelRDD,
      TableSchemaHelper.SCHEMA_PROVINCE_RISK_LEVEL)
    provinceRiskLevelDF.show()
    provinceRiskLevelDF.write.mode(SaveMode.Append).jdbc(sqlUrl, SqlStringUtils.STR_TAB_PROVINCE_RISK_LEVEL, properties)

    // 计算各个省份的personal rank
    var provinceMigrationPersonalRankRDD: RDD[Row]
    = spark.sparkContext.emptyRDD[Row]
    val locationIds = ProvinceUtils.IdtoProvinceMap.keys
    for(key<-locationIds) {
      val tempRDD = GraphProcess.processSingleProvincePersonalRank(spark,dateStand,isIn,key,
        vertexRDD, edgeRDD, properties, sqlUrl)
      provinceMigrationPersonalRankRDD = provinceMigrationPersonalRankRDD.union(tempRDD)
    }
    println(provinceMigrationPersonalRankRDD.count())
    //   统一建立DF并保存到数据库
    val provinceMigrationPersonalRankDF = spark.createDataFrame(provinceMigrationPersonalRankRDD,
          TableSchemaHelper.SCHEMA_PROVINCE_MIGRATION_PERSONAL_RANK)
    provinceMigrationPersonalRankDF.show()
    provinceMigrationPersonalRankDF.write.mode(SaveMode.Append).jdbc(sqlUrl,
      SqlStringUtils.STR_TAB_PROVINCE_MIGRATION_PERSONAL_RANK, properties)
  }

  def processSingleProvincePersonalRank(spark:SparkSession, datetime:String, isIn:Boolean, locationId:String,
                                        vertexRDD:RDD[(Long, String)], edgeRDD:RDD[Edge[Double]], properties:Properties,
                                        sqlUrl:String): RDD[Row] = {
    val personalRankRDD = Graph(vertexRDD, edgeRDD).ops.
      staticPersonalizedPageRank(locationId.toLong, 5, 0.15).vertices
    println(personalRankRDD)
    val weightTotal = personalRankRDD.map(f=>f._2).fold(0)(_ + _)
    val weightedPersonalRankRDD = personalRankRDD.map(f=>Row(datetime,locationId.toString,
      f._1.toString, isIn, f._2/weightTotal))
    println(weightedPersonalRankRDD)
    return weightedPersonalRankRDD
  }

}
