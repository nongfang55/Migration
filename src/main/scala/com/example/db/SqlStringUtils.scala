package com.example.db

object SqlStringUtils {
  // 用于保存各种作用的sql语句
  val STR_SQL_QUERY_FROM_MIGRATION_WITH_DATE = "select * from migration where date >= ? and date <= ?"


  // 会用到的表名
  val STR_TAB_MIGRATION = "migration"
  val STR_TAB_PROVINCE_MIGRATION_PAGE_RANK = "provinceMigrationPageRank"
  val STR_TAB_PROVINCE_MIGRATION_TREND = "provinceMigrationTrend"
  val STR_TAB_PROVINCE_MIGRATION_DETAIL = "provinceMigrationDetail"
  val STR_TAB_PROVINCE_TREND = "provincetrend"
  val STR_TAB_PROVINCE_RISK_LEVEL = "provinceRiskLevel"
  val STR_TAB_PROVINCE_MIGRATION_PERSONAL_RANK = "provinceMigrationPersonalRank"


  // sql 查询会用到的 属性
  val STR_KEY_USER = "user"
  val STR_KEY_PASSWORD = "password"
  val STR_KEY_DRIVER = "driver"

}
