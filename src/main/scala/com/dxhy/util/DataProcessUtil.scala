package com.dxhy.util

import com.alibaba.fastjson.JSON
import com.dxhy.constants.InvoiceConstants
import com.dxhy.entity.Invoice4Solr
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * 数据处理工具类
  * Created by drguo on 2017/11/23.
  */
object DataProcessUtil {

  /**
    * 将json数据转为solr索引类
    * @param json json格式字符串
    * @return
    */
  def generateInvoice4Solr(json: String): Invoice4Solr = {
    val jsonObject = JSON.parseObject(json)
    val dataObject = jsonObject.getJSONObject(InvoiceConstants.DATA)
    val fpkj = dataObject.getJSONObject(InvoiceConstants.FP_KJ)
    val FPQQLSH = fpkj.getString(InvoiceConstants.FPQQLSH)
    val FP_DM = fpkj.getString(InvoiceConstants.FP_DM)
    val FP_HM = fpkj.getString(InvoiceConstants.FP_HM)
    val KPRQ: Long = fpkj.getString(InvoiceConstants.KPRQ).toLong
    val DDH = fpkj.getString(InvoiceConstants.DDH)
    val DDSJ = fpkj.getString(InvoiceConstants.DDSJ)
    val XSF_NSRSBH = fpkj.getString(InvoiceConstants.XSF_NSRSBH)
    val XSF_MC = fpkj.getString(InvoiceConstants.XSF_MC)
    val XSF_DZDH = fpkj.getString(InvoiceConstants.XSF_DZDH)
    val GMF_MC = fpkj.getString(InvoiceConstants.GMF_MC)
    val GMF_NSRSBH = fpkj.getString(InvoiceConstants.GMF_NSRSBH)
    val GMF_SJ = fpkj.getString(InvoiceConstants.GMF_SJ)
    val GMF_WX = fpkj.getString(InvoiceConstants.GMF_WX)
    val YFP_DM = fpkj.getString(InvoiceConstants.YFP_DM)
    val YFP_HM = fpkj.getString(InvoiceConstants.YFP_HM)
    val HJJE = fpkj.getString(InvoiceConstants.HJJE)
    val HJSE = fpkj.getString(InvoiceConstants.HJSE)
    val SWJG_DM = fpkj.getString(InvoiceConstants.SWJG_DM)
    val FPZL_DM = fpkj.getString(InvoiceConstants.FPZL_DM)
    val HY_DM = fpkj.getString(InvoiceConstants.HY_DM)
    val GMF_EMAIL = fpkj.getString(InvoiceConstants.GMF_EMAIL)
    val JSHJ = fpkj.getString(InvoiceConstants.JSHJ)
    val JYM = fpkj.getString(InvoiceConstants.JYM)
    val FPLB = fpkj.getString(InvoiceConstants.FPLB)
    val DSPTBM = fpkj.getString(InvoiceConstants.DSPTBM)
    val invoice4Solr = new Invoice4Solr
    invoice4Solr.setId(MD5Util.MD5Encode(FP_DM + FP_HM))
    invoice4Solr.setFPQQLSH(FPQQLSH)
    invoice4Solr.setFP_DM(FP_DM)
    invoice4Solr.setFP_HM(FP_HM)
    invoice4Solr.setKPRQ(KPRQ)
    invoice4Solr.setDDH(DDH)
    invoice4Solr.setDDSJ(DDSJ)
    invoice4Solr.setXSF_NSRSBH(XSF_NSRSBH)
    invoice4Solr.setXSF_MC(XSF_MC)
    invoice4Solr.setXSF_DZDH(XSF_DZDH)
    invoice4Solr.setGMF_MC(GMF_MC)
    invoice4Solr.setGMF_NSRSBH(GMF_NSRSBH)
    invoice4Solr.setGMF_SJ(GMF_SJ)
    invoice4Solr.setGMF_WX(GMF_WX)
    invoice4Solr.setYFP_DM(YFP_DM)
    invoice4Solr.setYFP_HM(YFP_HM)
    invoice4Solr.setHJJE(HJJE)
    invoice4Solr.setHJSE(HJSE)
    invoice4Solr.setSWJG_DM(SWJG_DM)
    invoice4Solr.setFPZL_DM(FPZL_DM)
    invoice4Solr.setHY_DM(HY_DM)
    invoice4Solr.setGMF_EMAIL(GMF_EMAIL)
    invoice4Solr.setJSHJ(JSHJ)
    invoice4Solr.setJYM(JYM)
    invoice4Solr.setFPLB(FPLB)
    invoice4Solr.setDSPTBM(DSPTBM)
    invoice4Solr
  }

  /**
    * 将传入json数据转化为hbase get
    * @param json
    * @return
    */
  def generatePut(json: String): Put = {
    val jsonObject = JSON.parseObject(json)
    val dataObject = jsonObject.getJSONObject("Data")
    val fpkj = dataObject.getJSONObject("FP_KJ")
    val cover = jsonObject.getString("Cover")
    val `type` = jsonObject.getString("Type")
    val addTime = String.valueOf(System.currentTimeMillis)
    //fpkj.remove(InvoiceConstants.FILE_CONTENT);
    val kprq = fpkj.getString("KPRQ")
    val fpdm = fpkj.getString("FP_DM")
    val fphm = fpkj.getString("FP_HM")
    val rowKey = MD5Util.MD5Encode(fpdm + fphm)
    val put = new Put(Bytes.toBytes(rowKey))
    //addColumn(byte[] family, byte[] qualifier, byte[] value)
    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("KPRQ"), Bytes.toBytes(kprq))
    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Type"), Bytes.toBytes(`type`))
    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Cover"), Bytes.toBytes(cover))
    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Data"), Bytes.toBytes(jsonObject.toJSONString))
    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("addTime"), Bytes.toBytes(addTime))
    put
  }

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
