package com.example.bean

case class Migration(var flag:String, //0为迁入/1为迁出
                     var sprovinceId:String, //id不为“000000”代表为省份id，“”0000000“为国家数据
                     var province_name:String, //省份名称
                     var mproId:String, //迁入/迁出省份id
                     var value:String, //迁徙比例
                     var datetime:String //记录时间
)