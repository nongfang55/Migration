package com.example.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

import scala.collection.mutable.ArrayBuffer

object DateConvertHelper {
   // 用于转化时间字符串
    def convertStandTime(datetime: String): String = {
      if(datetime.length == 8) {
        val year = datetime.substring(0, 4)
        val month = datetime.substring(4, 6)
        val day = datetime.substring(6)
        return year + '-' + month + '-' + day
      }
      return datetime // 不符合格式的日期原路返回
    }

  def convertStandTimeToRaw(datetime: String): String = {
    if(datetime.length == 10) {
      val year = datetime.substring(0, 4)
      val month = datetime.substring(5, 7)
      val day = datetime.substring(8)
      return year + month + day
    }
    return datetime // 不符合格式的日期原路返回
  }

  def generateDateList(startDate:String, endDate:String): Array[String] = {
    // 通过输入日期的起始和截至日期，返回对应的时间序列字符串
    val simpleDateFormat = new SimpleDateFormat(StringKeyUtils.STR_KEY_DATE_FORMAT_WITH_NO_LINE)
    var date_s = simpleDateFormat.parse(startDate)
    val date_e = simpleDateFormat.parse(endDate)
    val dateArray = ArrayBuffer[String]()
    val calendar = new GregorianCalendar()
    while(date_s.compareTo(date_e) < 1) {
      dateArray.append(simpleDateFormat.format(date_s))
      calendar.setTime(date_s)
      calendar.add(Calendar.DATE, 1)
      date_s = calendar.getTime
    }
    return dateArray.toArray
  }
}
