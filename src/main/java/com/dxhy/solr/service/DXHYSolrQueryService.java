package com.dxhy.solr.service;

import org.apache.solr.client.solrj.SolrServerException;

import java.util.List;
import java.util.Map;

public interface DXHYSolrQueryService {

    //1.根据识别号、订单号查询
    List<String> queryBySBHandOrder(String nsrsbh, String ddh) throws SolrServerException;

    //增加分页功能
    List<String> queryPageSBHandOrder(String nsrsbh, String ddh, boolean fp_kjmx, int pageNo, int pageSize) throws SolrServerException;
    
    //2.根据代码、号码查询
    List<String> queryByDMandHM(String fpdm, String fphm, int pageNo, int pageSize) throws SolrServerException;

    //FILE_CONTENT
    List<String> queryByDM(String fpdm, boolean fpkjmx, int pageNo, int pageSize) throws SolrServerException;

    //3.原代码、原号码查询
    List<String> queryByYDMandYHM(String yfpdm, String yfphm, int pageNo, int pageSize) throws SolrServerException;

    //4.根据用户手机号和开票日期
    List<String> queryBySJandDate(String phoneNum, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;

    List<String> queryBySJandDate(String[] phoneNums, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;

    //5.根据用户邮箱和开票日期范围进行查询
    List<String> queryByMailandDate(String mail, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;

    List<String> queryByMailandDate(String[] mails, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;

    //XSF_NSRSBH、KPRQ_Q、KPRQ_Z、JSHJ、GMF_MC、JYM
    List<String> queryBySBHandDate(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException;

    //GMF_SJ、GMF_EMAIL、GMF_WX、GMF_QQ、GMF_WEIBO单个或者任意组合都可以查询
    List<String> queryByArgs(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException;
    
    
    //******************************V2.0***************************************
    //1.根据识别号、订单号查询
    //增加分页功能
    List<String> queryPageSBHandOrder2(String nsrsbh, String ddh, boolean fp_kjmx, int pageNo, int pageSize) throws SolrServerException;

    //3.原代码、原号码查询
    List<String> queryByYDMandYHM2(String yfpdm, String yfphm, int pageNo, int pageSize) throws SolrServerException;
    
    //4.根据用户手机号和开票日期
    List<String> queryBySJandDates2(String[] phoneNums, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;

    //5.根据用户邮箱和开票日期范围进行查询
    List<String> queryByMailandDates2(String[] mails, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException;
	
    //6.XSF_NSRSBH、KPRQ_Q、KPRQ_Z、JSHJ、GMF_MC、JYM
    List<String> queryBySBHandDate2(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException;

    //7.GMF_SJ、GMF_EMAIL、GMF_WX、GMF_QQ、GMF_WEIBO单个或者任意组合都可以查询
    List<String> queryByArgs2(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException;

    //8.多DSPTBM、DDH
    List<String> queryByDSPTBMandDDH2(String[] dsptbm, String ddh, int pageNo, int pageSize) throws SolrServerException;

}
