package com.example.utils

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
}
