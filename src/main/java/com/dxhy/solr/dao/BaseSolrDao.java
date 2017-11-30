package com.dxhy.solr.dao;

import com.alibaba.fastjson.JSONObject;
import com.dxhy.constants.InvoiceConstants;
import com.dxhy.entity.Invoice4Solr;
import com.dxhy.util.MD5Util;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;

public class BaseSolrDao {

    //************************************V1.0***********************
    //返回当前页的信息
    public QueryResponse getPageResponse(String sql, int start, long size, String fSql) throws SolrServerException {
        SolrQuery filterQuery = new SolrQuery();
        filterQuery.set("collection", ClusterSolrDao.COLLECTION);
        //按照日期查询过滤
        filterQuery.setQuery(sql);
        if (!"".equalsIgnoreCase(fSql)) {
            filterQuery.addFilterQuery(fSql);
        }
        filterQuery.setSort(InvoiceConstants.KPRQ, ORDER.desc);
        if (start > 0) {
            filterQuery.setStart((int) size * (start - 1));
        }
        if (size > 0) {
            filterQuery.setRows((int) size);
        }
        return ClusterSolrDao.queryServer.query(filterQuery);
    }

    //查看总页数
    public long totalPage(QueryResponse response) throws SolrServerException {

        return response.getResults().getNumFound();
    }

    //解析
    public List<String> analyzeResponse(QueryResponse response) throws SolrServerException {
        long total = totalPage(response);
        List<String> l = new ArrayList<String>();
        l.add(String.valueOf(total));
        if (response != null) {
            SolrDocumentList list = response.getResults();
            for (int i = 0; i < list.size(); i++) {
                String id = list.get(i).getFieldValue("id").toString();//根据key取值
                l.add(id);
            }
        }
        return l;
    }

    //************************************V2.0***********************
    //返回当前页的信息
    public QueryResponse getPageResponse(String collections, String sql, int start, long size, String fSql) throws SolrServerException {
        SolrQuery filterQuery = new SolrQuery();
        //按照日期查询过滤
        filterQuery.add("collection", collections);
        filterQuery.addField("id");
        filterQuery.setQuery(sql);
        if (!"".equalsIgnoreCase(fSql)) {
            filterQuery.addFilterQuery(fSql);
        }
        filterQuery.setSort(InvoiceConstants.KPRQ, ORDER.desc);

        if (start > 0) {
            filterQuery.setStart((int) size * (start - 1));
        }
        if (size > 0) {
            filterQuery.setRows((int) size);
        }
        QueryResponse response = ClusterSolrDao.queryServer.query(filterQuery, METHOD.POST);
        return response;
    }

    public static Invoice4Solr generateInvoice4Solr(String json) {

        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject dataObject = jsonObject.getJSONObject(InvoiceConstants.DATA);
        JSONObject fpkj = dataObject.getJSONObject(InvoiceConstants.FP_KJ);

        String FPQQLSH = fpkj.getString(InvoiceConstants.FPQQLSH);
        String FP_DM = fpkj.getString(InvoiceConstants.FP_DM);
        String FP_HM = fpkj.getString(InvoiceConstants.FP_HM);
        long KPRQ = Long.valueOf(fpkj.getString(InvoiceConstants.KPRQ));
        String DDH = fpkj.getString(InvoiceConstants.DDH);
        String DDSJ = fpkj.getString(InvoiceConstants.DDSJ);
        String XSF_NSRSBH = fpkj.getString(InvoiceConstants.XSF_NSRSBH);
        String XSF_MC = fpkj.getString(InvoiceConstants.XSF_MC);
        String XSF_DZDH = fpkj.getString(InvoiceConstants.XSF_DZDH);
        String GMF_MC = fpkj.getString(InvoiceConstants.GMF_MC);
        String GMF_NSRSBH = fpkj.getString(InvoiceConstants.GMF_NSRSBH);
        String GMF_SJ = fpkj.getString(InvoiceConstants.GMF_SJ);
        String GMF_WX = fpkj.getString(InvoiceConstants.GMF_WX);
        String YFP_DM = fpkj.getString(InvoiceConstants.YFP_DM);
        String YFP_HM = fpkj.getString(InvoiceConstants.YFP_HM);
        String HJJE = fpkj.getString(InvoiceConstants.HJJE);
        String HJSE = fpkj.getString(InvoiceConstants.HJSE);
        String SWJG_DM = fpkj.getString(InvoiceConstants.SWJG_DM);
        String FPZL_DM = fpkj.getString(InvoiceConstants.FPZL_DM);
        String HY_DM = fpkj.getString(InvoiceConstants.HY_DM);
        String GMF_EMAIL = fpkj.getString(InvoiceConstants.GMF_EMAIL);
        String JSHJ = fpkj.getString(InvoiceConstants.JSHJ);
        String JYM = fpkj.getString(InvoiceConstants.JYM);
        String FPLB = fpkj.getString(InvoiceConstants.FPLB);
        String DSPTBM = fpkj.getString(InvoiceConstants.DSPTBM);

        Invoice4Solr invoice4Solr = new Invoice4Solr();
        invoice4Solr.setId(MD5Util.MD5Encode(FP_DM + FP_HM));
        invoice4Solr.setFPQQLSH(FPQQLSH);
        invoice4Solr.setFP_DM(FP_DM);
        invoice4Solr.setFP_HM(FP_HM);
        invoice4Solr.setKPRQ(KPRQ);
        invoice4Solr.setDDH(DDH);
        invoice4Solr.setDDSJ(DDSJ);
        invoice4Solr.setXSF_NSRSBH(XSF_NSRSBH);
        invoice4Solr.setXSF_MC(XSF_MC);
        invoice4Solr.setXSF_DZDH(XSF_DZDH);
        invoice4Solr.setGMF_MC(GMF_MC);
        invoice4Solr.setGMF_NSRSBH(GMF_NSRSBH);
        invoice4Solr.setGMF_SJ(GMF_SJ);
        invoice4Solr.setGMF_WX(GMF_WX);
        invoice4Solr.setYFP_DM(YFP_DM);
        invoice4Solr.setYFP_HM(YFP_HM);
        invoice4Solr.setHJJE(HJJE);
        invoice4Solr.setHJSE(HJSE);
        invoice4Solr.setSWJG_DM(SWJG_DM);
        invoice4Solr.setFPZL_DM(FPZL_DM);
        invoice4Solr.setHY_DM(HY_DM);
        invoice4Solr.setGMF_EMAIL(GMF_EMAIL);
        invoice4Solr.setJSHJ(JSHJ);
        invoice4Solr.setJYM(JYM);
        invoice4Solr.setFPLB(FPLB);
        invoice4Solr.setDSPTBM(DSPTBM);

        return invoice4Solr;
    }


    public static String generateInvoice4SolrJson(String json) {


        JSONObject solrJsonObject = new JSONObject();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject dataObject = jsonObject.getJSONObject(InvoiceConstants.DATA);
        JSONObject fpkj = dataObject.getJSONObject(InvoiceConstants.FP_KJ);

        String FPQQLSH = fpkj.getString(InvoiceConstants.FPQQLSH);
        String FP_DM = fpkj.getString(InvoiceConstants.FP_DM);
        String FP_HM = fpkj.getString(InvoiceConstants.FP_HM);
        String KPRQ = fpkj.getString(InvoiceConstants.KPRQ);
        String DDH = fpkj.getString(InvoiceConstants.DDH);
        String DDSJ = fpkj.getString(InvoiceConstants.DDSJ);
        String XSF_NSRSBH = fpkj.getString(InvoiceConstants.XSF_NSRSBH);
        String XSF_MC = fpkj.getString(InvoiceConstants.XSF_MC);
        String XSF_DZDH = fpkj.getString(InvoiceConstants.XSF_DZDH);
        String GMF_MC = fpkj.getString(InvoiceConstants.GMF_MC);
        String GMF_NSRSBH = fpkj.getString(InvoiceConstants.GMF_NSRSBH);
        String GMF_SJ = fpkj.getString(InvoiceConstants.GMF_SJ);
        String GMF_WX = fpkj.getString(InvoiceConstants.GMF_WX);
        String YFP_DM = fpkj.getString(InvoiceConstants.YFP_DM);
        String YFP_HM = fpkj.getString(InvoiceConstants.YFP_HM);
        String HJJE = fpkj.getString(InvoiceConstants.HJJE);
        String HJSE = fpkj.getString(InvoiceConstants.HJSE);
        String SWJG_DM = fpkj.getString(InvoiceConstants.SWJG_DM);
        String FPZL_DM = fpkj.getString(InvoiceConstants.FPZL_DM);
        String HY_DM = fpkj.getString(InvoiceConstants.HY_DM);
        String GMF_EMAIL = fpkj.getString(InvoiceConstants.GMF_EMAIL);
        String JSHJ = fpkj.getString(InvoiceConstants.JSHJ);
        String JYM = fpkj.getString(InvoiceConstants.JYM);
        String FPLB = fpkj.getString(InvoiceConstants.FPLB);
        String DSPTBM = fpkj.getString(InvoiceConstants.DSPTBM);

        solrJsonObject.put("id", MD5Util.MD5Encode(FP_DM + FP_HM));
        solrJsonObject.put(InvoiceConstants.FPQQLSH, FPQQLSH);
        solrJsonObject.put(InvoiceConstants.FP_DM, FP_DM);
        solrJsonObject.put(InvoiceConstants.FP_HM, FP_HM);
        solrJsonObject.put(InvoiceConstants.KPRQ, KPRQ);
        solrJsonObject.put(InvoiceConstants.DDH, DDH);
        solrJsonObject.put(InvoiceConstants.DDSJ, DDSJ);
        solrJsonObject.put(InvoiceConstants.XSF_NSRSBH, XSF_NSRSBH);
        solrJsonObject.put(InvoiceConstants.XSF_MC, XSF_MC);
        solrJsonObject.put(InvoiceConstants.XSF_DZDH, XSF_DZDH);
        solrJsonObject.put(InvoiceConstants.GMF_MC, GMF_MC);
        solrJsonObject.put(InvoiceConstants.GMF_NSRSBH, GMF_NSRSBH);
        solrJsonObject.put(InvoiceConstants.GMF_SJ, GMF_SJ);
        solrJsonObject.put(InvoiceConstants.GMF_WX, GMF_WX);
        solrJsonObject.put(InvoiceConstants.YFP_DM, YFP_DM);
        solrJsonObject.put(InvoiceConstants.YFP_HM, YFP_HM);
        solrJsonObject.put(InvoiceConstants.HJJE, HJJE);
        solrJsonObject.put(InvoiceConstants.HJSE, HJSE);
        solrJsonObject.put(InvoiceConstants.SWJG_DM, SWJG_DM);
        solrJsonObject.put(InvoiceConstants.FPZL_DM, FPZL_DM);
        solrJsonObject.put(InvoiceConstants.HY_DM, HY_DM);
        solrJsonObject.put(InvoiceConstants.GMF_EMAIL, GMF_EMAIL);
        solrJsonObject.put(InvoiceConstants.JSHJ, JSHJ);
        solrJsonObject.put(InvoiceConstants.JYM, JYM);
        solrJsonObject.put(InvoiceConstants.FPLB, FPLB);
        solrJsonObject.put(InvoiceConstants.DSPTBM, DSPTBM);

        return solrJsonObject.toJSONString();
    }


    //查询  collections 获取
    public String getCollections(String startDate, String endDate) {

        List<String> copyCollections = new LinkedList<String>();
        copyCollections.addAll(Arrays.asList(ClusterSolrDao.COLLECTIONS));
        int start = 0;
        int end = 0;
        startDate = ClusterSolrDao.COLLECTION_PREFIX + startDate;
        endDate = ClusterSolrDao.COLLECTION_PREFIX + endDate;

        if (!copyCollections.contains(startDate) && !copyCollections.contains(endDate)) {

            copyCollections.add(startDate);
            Collections.sort(copyCollections);
            start = Collections.binarySearch(copyCollections, startDate) - 1;
            copyCollections.remove(startDate);
            copyCollections.add(endDate);
            Collections.sort(copyCollections);
            end = Collections.binarySearch(copyCollections, endDate) - 1;

        } else if (!copyCollections.contains(startDate) && copyCollections.contains(endDate)) {

            copyCollections.add(startDate);
            Collections.sort(copyCollections);
            start = Collections.binarySearch(copyCollections, startDate) - 1;
            copyCollections.remove(startDate);
            Collections.sort(copyCollections);
            end = Collections.binarySearch(copyCollections, endDate);

        } else if (copyCollections.contains(startDate) && !copyCollections.contains(endDate)) {

            Collections.sort(copyCollections);
            start = Collections.binarySearch(copyCollections, startDate);
            copyCollections.add(endDate);
            Collections.sort(copyCollections);
            end = Collections.binarySearch(copyCollections, endDate) - 1;

        } else if (copyCollections.contains(startDate) && copyCollections.contains(endDate)) {

            Collections.sort(copyCollections);
            start = Collections.binarySearch(copyCollections, startDate);
            end = Collections.binarySearch(copyCollections, endDate);

        }

        StringBuilder sb = new StringBuilder();
        for (int i = start; i <= end; i++) {

            if (i == end) {
                sb.append(ClusterSolrDao.COLLECTIONS[i]);
            } else {
                sb.append(ClusterSolrDao.COLLECTIONS[i]).append(",");
            }

        }

        //不考虑时间collection
//        sb.append(ClusterSolrDao.COLLECTION);

        return sb.toString();
    }

    //查询  collection 获取
    public String getCollection(String kprq) {

        List<String> copyCollections = new ArrayList<String>();
        copyCollections.addAll(Arrays.asList(ClusterSolrDao.COLLECTIONS));
        String date = ClusterSolrDao.COLLECTION_PREFIX + kprq;
        int index = 0;
        if (copyCollections.contains(date)) {

            Collections.sort(copyCollections);
            index = Collections.binarySearch(copyCollections, date);

        } else if (!copyCollections.contains(date)) {

            copyCollections.add(date);
            Collections.sort(copyCollections);
            index = Collections.binarySearch(copyCollections, date) - 1;

        }

        return ClusterSolrDao.COLLECTIONS[index];
    }

}
