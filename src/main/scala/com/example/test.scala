package com.example

import com.example.config.ConfigParser
import com.example.utils.{DateConvertHelper, ProvinceUtils, StringKeyUtils}

object test {
  def main(args:Array[String]) : Unit = {
    println("hello worlds")

//    val properties = ConfigParser.loadProperties(StringKeyUtils.STR_KEY_PROPERTIES_FILE_NAME)
//    print(properties)
//
//    println(properties.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_DRIVER))
//    println(properties.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_URL))
//    println(properties.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_USER_NAME))
//    println(properties.getProperty(ConfigParser.STR_KEY_CONFIG_DATABASE_USER_PASSWORD))
//
//    println(DateConvertHelper.convertStandTime("20201010"))
//    println(DateConvertHelper.convertStandTimeToRaw("2020-10-10"))

    DateConvertHelper.generateDateList("20200119", "20200301")
  }
}
