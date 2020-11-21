package com.example.service

import java.util.Properties

import com.example.config.ConfigParser
import com.example.db.SqlStringUtils
import com.example.utils.{ProvinceUtils, StringKeyUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object EpidemicDataLoader {
  // 用于加载数据库的疫情日期,并以比较规整的形式返回

  def loadEpidemicData(spark:SparkSession, sqlUrl:String, properties:Properties): RDD[(String, String, Long)] = {
    // 读取数据库
    val resultDs = spark.read.jdbc(sqlUrl, SqlStringUtils.STR_TAB_PROVINCE_TREND, properties)
    resultDs.show()
    val resultRD = resultDs.rdd.map(f=>(f.getString(0), f.getLong(1).toString, f.getLong(3)))
    return resultRD
  }

  def NormalizeEpidemicRDByDate(spark:SparkSession, epidemicRawRDD:RDD[(String, String, Long)],
                                datetime:String): RDD[(String, Double)] = {
    // 依据时间过滤
    val filterEpidemicRDD = epidemicRawRDD.filter(f=>f._1.equals(datetime)).map(f=>(f._2, f._3))
    // 建立所有省份的默认RDD确证集合
    val config = ConfigParser.loadProperties(StringKeyUtils.STR_KEY_PROPERTIES_FILE_NAME)
    val offset = config.getProperty(ConfigParser.STR_KEY_CONFIG_EPIDEMIC_OFFSET).toLong
    val epidemicOffsetRDD:RDD[(String, Long)] = spark.sparkContext.
      parallelize(ProvinceUtils.IdtoProvinceMap.keys.map(f=>(f, offset)).toSeq)
    val tempJoinRDD = epidemicOffsetRDD.leftOuterJoin(filterEpidemicRDD)
    tempJoinRDD.foreach(println)
    tempJoinRDD.collect()
    val logEpidemicOffsetRDD  = tempJoinRDD.map(f=>(f._1, f._2._1 + (if(f._2._2.isInstanceOf[Some[Long]]) f._2._2.get else 0)))
      .map(f=>(f._1, Math.log(f._2.toDouble)))
    return logEpidemicOffsetRDD
  }
}
