package com.dxhy.util

import com.alibaba.fastjson.JSON

/**
  * 数据处理工具类
  * Created by drguo on 2017/11/23.
  */
object DataProcessUtil {

  /**
    * json 数据有效性检验
    * @param strJson json格式字符串
    * @return true代表有效
    */
  def jsonDataValidate(strJson: String): Boolean = {
    var isValidate = true
    try {
      val jsonData = JSON.parseObject(strJson)
      val jsonFP = jsonData.getJSONObject("Data").getJSONObject("FP_KJ")
      //销售方名称
      val nsrmc = jsonFP.getString("XSF_MC")
      //开票日期
      val kprq = jsonFP.getString("KPRQ")
      //销售方纳税人识别号
      val nsrsbh = jsonFP.getString("XSF_NSRSBH")
      //发票代码
      val fpdm = jsonFP.getString("FP_DM")

      if (strJson == null || strJson.isEmpty || nsrmc == null || nsrmc.isEmpty || kprq == null || kprq.isEmpty
        || nsrsbh == null || nsrsbh.isEmpty || fpdm == null || fpdm.isEmpty) {

        isValidate = false
      }
    } catch {
      case e: Exception => println(e)
        isValidate = false
    }
    isValidate
  }
  /*lines.filter{line =>
    val jsonData = JSON.parseObject(line)
    val jsonFP = jsonData.getJSONObject("Data").getJSONObject("FP_KJ")
    //销售方名称
    val nsrmc = jsonFP.getString("XSF_MC")
    //开票日期
    val kprq = jsonFP.getString("KPRQ")
    //销售方纳税人识别号
    val nsrsbh = jsonFP.getString("XSF_NSRSBH")
    //发票代码
    val fpdm = jsonFP.getString("FP_DM")

    val flag = !(nsrmc == null || nsrmc.isEmpty || kprq == null || kprq.isEmpty
      || nsrsbh == null || nsrsbh.isEmpty || fpdm == null || fpdm.isEmpty)
    flag*/
}
