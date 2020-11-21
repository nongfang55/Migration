package com.example.config

import java.util.Properties

import breeze.linalg.Options.Value

object ConfigParser {
  // 用于加载配置文件

  val STR_KEY_CONFIG_DATABASE_USER_NAME = "database_user_name"
  val STR_KEY_CONFIG_DATABASE_USER_PASSWORD = "database_user_password"
  val STR_KEY_CONFIG_DATABASE_URL = "database_url"
  val STR_KEY_CONFIG_DATABASE_DRIVER = "database_driver"
  val STR_KEY_CONFIG_EPIDEMIC_OFFSET = "epidemic_offset"

  def loadProperties(filePath: String): Properties = {
    val properties = new Properties()
    try {
      properties.load(ConfigParser.getClass.getClassLoader.getResourceAsStream(filePath))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    properties
  }

}
