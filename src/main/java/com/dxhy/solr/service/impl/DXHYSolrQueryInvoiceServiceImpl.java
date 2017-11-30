package com.dxhy.solr.service.impl;

import com.dxhy.solr.dao.BaseSolrDao;
import com.dxhy.solr.dao.ClusterSolrDao;
import com.dxhy.solr.service.DXHYSolrQueryService;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by thinkpad on 2017/2/28.
 */
@Service
public class DXHYSolrQueryInvoiceServiceImpl extends BaseSolrDao implements DXHYSolrQueryService {

    private static final Logger log = getLogger(DXHYSolrQueryInvoiceServiceImpl.class);

    public List<String> queryBySBHandOrder(String nsrsbh, String ddh) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("XSF_NSRSBH : " + nsrsbh + " AND DDH : " + ddh);
        response = getPageResponse(params.toString(), 1, 10, "");
        return analyzeResponse(response);
    }

    public List<String> queryPageSBHandOrder(String nsrsbh, String ddh, boolean fp_kjmx, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("XSF_NSRSBH : " + nsrsbh + " AND DDH : " + ddh);
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryByDMandHM(String fpdm, String fphm, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("FP_DM : " + fpdm + " AND  FP_HM : " + fphm);
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryByDM(String fpdm, boolean fpkjmx, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("FP_DM : " + fpdm);
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryByYDMandYHM(String yfpdm, String yfphm, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("YFP_DM : " + yfpdm + " AND YFP_HM : " + yfphm);
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryBySJandDate(String phoneNum, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("GMF_SJ : " + phoneNum);
        StringBuilder fParams = new StringBuilder("KPRQ:[" + startDate + " TO " + endDate + "]");
        response = getPageResponse(params.toString(), pageNo, pageSize, fParams.toString());
        return analyzeResponse(response);
    }

    public List<String> queryBySJandDate(String[] phoneNums, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("( GMF_SJ : ");
        int l = phoneNums.length;
        int j = 0;
        for (String phonenum : phoneNums) {
            j++;
            if (j == l) {
                params.append(phonenum.trim() + ") AND KPRQ:[" + startDate + " TO " + endDate + "]");
            } else {
                params.append(phonenum.trim() + " OR GMF_SJ :");
            }
        }
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryByMailandDate(String mail, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("GMF_EMAIL: " + mail);
        StringBuilder fParams = new StringBuilder("KPRQ:[" + startDate + " TO " + endDate + "]");
        response = getPageResponse(params.toString(), pageNo, pageSize, fParams.toString());
        return analyzeResponse(response);
    }

    public List<String> queryByMailandDate(String[] mails, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("( GMF_EMAIL : ");
        int l = mails.length;
        int j = 0;
        for (String phonenum : mails) {
            j++;
            if (j == l) {
                params.append(phonenum + " ) AND KPRQ:[" + startDate + " TO " + endDate + "]");
            } else {
                params.append(phonenum + " OR GMF_EMAIL :");
            }
        }
        response = getPageResponse(params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryBySBHandDate(String nsrsbh, String startDate, String endDate, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("XSF_NSRSBH: " + nsrsbh);
        StringBuilder fParams = new StringBuilder("KPRQ:[" + startDate + " TO " + endDate + "]");
        response = getPageResponse(params.toString(), pageNo, pageSize, fParams.toString());

        return analyzeResponse(response);
    }


    public List<String> queryByArgs(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        String GMF_SJ = "";
        String GMF_EMAIL = "";
        String GMF_WX = "";
        String GMF_QQ = "";
        String GMF_WEIBO = "";
        String KPRQ_Q = "";
        String KPRQ_Z = "";
        String FPLB = "";
        String DSPTBM = "";
        String solrSql = "";
        String solrSql1 = "";
        for (String key : m.keySet()) {
            if (key.equals("GMF_SJ")) {
                GMF_SJ = m.get(key);
                log.info("===============map==========value=======" + GMF_SJ);
            } else if (key.equals("GMF_EMAIL")) {
                GMF_EMAIL = m.get(key);
            } else if (key.equals("GMF_WX")) {
                GMF_WX = m.get(key);
            } else if (key.equals("GMF_QQ")) {
                GMF_QQ = m.get(key);
            } else if (key.equals("GMF_WEIBO")) {
                GMF_WEIBO = m.get(key);
            } else if (key.equals("DSPTBM")) {
                GMF_WEIBO = m.get(key);
            } else if (key.equals("FPLB")) {
                FPLB = m.get(key);
            } else if (key.equals("KPRQ_Q")) {
                KPRQ_Q = m.get(key);
            } else if (key.equals("KPRQ_Z")) {
                KPRQ_Z = m.get(key);
            }
        }
        if ("".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
        } else if (!"".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
            solrSql1 = "  KPRQ:[" + KPRQ_Q + " TO " + "320150813162302" + "]";
        } else if (!"".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "  KPRQ:[" + KPRQ_Q + " TO " + KPRQ_Z + "]";
        } else if ("".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "   KPRQ:[" + 0 + " TO " + KPRQ_Z + "]";
        }
        if (!"".equals(FPLB)) {
            solrSql = solrSql + " AND FPLB:" + FPLB;
        }
        if (!"".equals(GMF_SJ)) {
            solrSql = solrSql + "OR GMF_SJ:" + GMF_SJ;
        }
        if (!"".equals(GMF_EMAIL)) {
            solrSql = solrSql + " OR GMF_EMAIL:" + GMF_EMAIL;
        }
        if (!"".equals(GMF_WX)) {
            solrSql = solrSql + "  OR GMF_WX:" + GMF_WX;
        }
        if (!"".equals(GMF_QQ)) {
            solrSql = solrSql + " OR GMF_QQ:" + GMF_QQ;
        }
        if (!"".equals(GMF_WEIBO)) {
            solrSql = solrSql + " OR GMF_WEIBO:" + GMF_WEIBO;
        }
        if (!"".equals(DSPTBM)) {
            solrSql = solrSql + " OR DSPTBM:" + GMF_WEIBO;
        }
        String sql = solrSql.replaceFirst("AND|OR", "");
        response = getPageResponse(sql, pageNo, pageSize, solrSql1);
        return analyzeResponse(response);
    }

    public List<String> queryBySBHandDate(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException {
        QueryResponse response = null;
        String XSF_NSRSBH = "";
        String KPRQ_Q = "";
        String KPRQ_Z = "";
        String JSHJ = "";
        String GMF_MC = "";
        String JYM = "";
        String solrSql = "";
        String solrSql1 = "";
        for (String key : m.keySet()) {
            if (key.equals("XSF_NSRSBH")) {
                XSF_NSRSBH = m.get(key);
            } else if (key.equals("JSHJ")) {
                JSHJ = m.get(key);
            } else if (key.equals("GMF_MC")) {
                GMF_MC = m.get(key);
            } else if (key.equals("JYM")) {
                JYM = m.get(key);
            } else if (key.equals("KPRQ_Q")) {
                KPRQ_Q = m.get(key);
            } else if (key.equals("KPRQ_Z")) {
                KPRQ_Z = m.get(key);
            }
        }
        if ("".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
        } else if (!"".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
            solrSql1 = "  KPRQ:[" + KPRQ_Q + " TO " + "320150813162302" + "]";
        } else if (!"".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "   KPRQ:[" + KPRQ_Q + " TO " + KPRQ_Z + "]";
        } else if ("".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "   KPRQ:[" + 0 + " TO " + KPRQ_Z + "]";
        }
        if (!"".equals(XSF_NSRSBH)) {
            solrSql = solrSql + " AND XSF_NSRSBH:" + XSF_NSRSBH;
        }
        if (!"".equals(JSHJ)) {
            solrSql = solrSql + " AND JSHJ:" + JSHJ;
        }
        if (!"".equals(GMF_MC)) {
            solrSql = solrSql + " AND GMF_MC:" + GMF_MC;
        }
        if (!"".equals(JYM)) {
            solrSql = solrSql + "  AND JYM:" + JYM;
        }

        String sql = solrSql.replaceFirst("AND", "");
        response = getPageResponse(sql, pageNo, pageSize, solrSql1);
        return analyzeResponse(response);
    }

    //*******************************V2.0***************************************//
    //**************************************************************************//
    //**************************************************************************//

    /**
     * 根据纳税人识别号和订单号查询，可以选择是否返回fp_kjmx
     *
     * @param nsrsbh
     * @param ddh
     * @param fp_kjmx
     * @param pageNo
     * @param pageSize
     * @return
     * @throws SolrServerException
     */
    public List<String> queryPageSBHandOrder2(String nsrsbh, String ddh, boolean fp_kjmx, int pageNo, int pageSize) throws SolrServerException {

        QueryResponse response = null;
//        StringBuilder params = new StringBuilder("XSF_NSRSBH : " + nsrsbh + " AND DDH : " + ddh);
//        response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS, params.toString(), pageNo, pageSize, "");

        StringBuilder params = new StringBuilder("XSF_NSRSBH : " + nsrsbh);
        String sql = "DDH : " + ddh;
        response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS, sql, pageNo, pageSize, params.toString());

        List<String> solrList = analyzeResponse(response);
        return solrList;

    }


    /**
     * 根据单个 多个手机号以及日期范围进行查询
     *
     * @param phoneNums
     * @param startDate
     * @param endDate
     * @param pageNo
     * @param pageSize
     * @return
     * @throws SolrServerException
     */
    public List<String> queryBySJandDates2(String[] phoneNums, String startDate, String endDate, int pageNo,
                                           int pageSize) throws SolrServerException {

        String collections = getCollections(startDate, endDate);
        QueryResponse response = null;

//        StringBuilder params = new StringBuilder("( GMF_SJ : ");
//        for (int i = 0; i < phoneNums.length; i++) {
//            if (i == phoneNums.length - 1) {
//                params.append(phoneNums[i].trim() + " ) AND KPRQ:[" + startDate + " TO " + endDate + "]");
//            } else {
//                params.append(phoneNums[i].trim() + " OR GMF_SJ : ");
//            }
//        }
//      response = getPageResponse(collections, params.toString(), pageNo, pageSize, "");

        StringBuilder params = new StringBuilder("( GMF_SJ : ");
        for (int i = 0; i < phoneNums.length; i++) {
            if (i == phoneNums.length - 1) {
                params.append(phoneNums[i].trim() + " )");
            } else {
                params.append(phoneNums[i].trim() + " OR GMF_SJ : ");
            }
        }

        String sql = "KPRQ:[" + startDate + " TO " + endDate + "]";
        response = getPageResponse(collections, params.toString(), pageNo, pageSize, sql);

        List<String> solrList = analyzeResponse(response);
        return solrList;

    }


    /**
     * 根据单个 多个 邮箱及日期范围进行查询
     *
     * @param mails
     * @param startDate
     * @param endDate
     * @param pageNo
     * @param pageSize
     * @return
     * @throws SolrServerException
     */
    public List<String> queryByMailandDates2(String[] mails, String startDate, String endDate, int pageNo, int pageSize)
            throws SolrServerException {

        String collections = getCollections(startDate, endDate);
        QueryResponse response = null;

//        StringBuilder params = new StringBuilder("( GMF_EMAIL : ");
//
//        for (int i = 0; i < mails.length; i++) {
//            if (i == mails.length - 1) {
//                params.append(mails[i].trim() + ") AND KPRQ:[" + startDate + " TO " + endDate + "]");
//            } else {
//                params.append(mails[i].trim() + " OR GMF_EMAIL :");
//            }
//        }
//      response = getPageResponse(collections, params.toString(), pageNo, pageSize, "");

        StringBuilder params = new StringBuilder("( GMF_EMAIL : ");
        for (int i = 0; i < mails.length; i++) {
            if (i == mails.length - 1) {
                params.append(mails[i].trim() + " )");
            } else {
                params.append(mails[i].trim() + " OR GMF_EMAIL : ");
            }
        }
        String sql = "KPRQ:[" + startDate + " TO " + endDate + "]";
        response = getPageResponse(collections, params.toString(), pageNo, pageSize, sql);

        List<String> solrList = analyzeResponse(response);
        return solrList;

    }


    /**
     * 根据销售方纳税人识别号 开票日期起止 （非必填：价税合计 校验码 购买方名称）查询
     *
     * @param m
     * @param pageNo
     * @param pageSize
     * @return
     * @throws SolrServerException
     */
    public List<String> queryBySBHandDate2(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException {

        QueryResponse response = null;

        String XSF_NSRSBH = "";
        String JSHJ = "";
        String GMF_MC = "";
        String JYM = "";
        String KPRQ_Q = "";
        String KPRQ_Z = "";
        String solrSql = "";
        String solrSql1 = "";

        if (m.containsKey("XSF_NSRSBH")) {
            XSF_NSRSBH = m.get("XSF_NSRSBH");
        }

        if (m.containsKey("JSHJ")) {
            JSHJ = m.get("JSHJ");
        }

        if (m.containsKey("GMF_MC")) {
            GMF_MC = m.get("GMF_MC");
        }

        if (m.containsKey("JYM")) {
            JYM = m.get("JYM");
        }

        if (m.containsKey("KPRQ_Q")) {
            KPRQ_Q = m.get("KPRQ_Q");
        }

        if (m.containsKey("KPRQ_Z")) {
            KPRQ_Z = m.get("KPRQ_Z");
        }

        if ("".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
        } else if (!"".equals(KPRQ_Q) && "".equals(KPRQ_Z)) {
            solrSql1 = "  KPRQ:[" + KPRQ_Q + " TO " + "320150813162302" + "]";
        } else if (!"".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "   KPRQ:[" + KPRQ_Q + " TO " + KPRQ_Z + "]";
        } else if ("".equals(KPRQ_Q) && !"".equals(KPRQ_Z)) {
            solrSql1 = "   KPRQ:[" + 0 + " TO " + KPRQ_Z + "]";
        }

        if (!"".equals(XSF_NSRSBH)) {
            solrSql = solrSql + " AND XSF_NSRSBH:" + XSF_NSRSBH;
        }
        if (!"".equals(JSHJ)) {
            solrSql = solrSql + " AND JSHJ:" + JSHJ;
        }
        if (!"".equals(GMF_MC)) {
            solrSql = solrSql + " AND GMF_MC:" + GMF_MC;
        }
        if (!"".equals(JYM)) {
            solrSql = solrSql + "  AND JYM:" + JYM;
        }


        String collections = getCollections(KPRQ_Q, KPRQ_Z);
        String sql = solrSql.replaceFirst("AND", "");//solr SQL
        response = getPageResponse(collections, sql, pageNo, pageSize, solrSql1);
        List<String> solrList = analyzeResponse(response);

        return solrList;
    }


    /**
     * 根据 日期起止 手机 邮箱 发票类别 电商平台编码查询
     *
     * @param m
     * @param pageNo
     * @param pageSize
     * @return
     * @throws SolrServerException
     */
    public List<String> queryByArgs2(Map<String, String> m, int pageNo, int pageSize) throws SolrServerException {

        QueryResponse response = null;

        String GMF_SJ = "";
        String GMF_EMAIL = "";
        String KPRQ_Q = "";
        String KPRQ_Z = "";
        String FPLB = "";
        String solrSql = "";
        String solrSql1 = "";

        if (m.containsKey("GMF_SJ")) {
            GMF_SJ = m.get("GMF_SJ");
        }

        if (m.containsKey("GMF_EMAIL")) {
            GMF_EMAIL = m.get("GMF_EMAIL");
        }

        if (m.containsKey("FPLB")) {
            FPLB = m.get("FPLB");
        }

        if (m.containsKey("KPRQ_Q")) {
            KPRQ_Q = m.get("KPRQ_Q");
        }

        if (m.containsKey("KPRQ_Z")) {
            KPRQ_Z = m.get("KPRQ_Z");
        }
        solrSql1 = " KPRQ:[" + KPRQ_Q + " TO " + KPRQ_Z + "]";

        if (!"".equals(GMF_SJ)) {
            solrSql = solrSql + "OR GMF_SJ:" + GMF_SJ;
        }
        if (!"".equals(GMF_EMAIL)) {
            solrSql = solrSql + " OR GMF_EMAIL:" + GMF_EMAIL;
        }
        if (!"".equals(FPLB)) {
            solrSql = solrSql + " AND FPLB:" + FPLB;
        }
        String sql = solrSql.replaceFirst("OR", "");
        if (sql.contains("GMF_SJ") && sql.contains("GMF_EMAIL")) {
            sql = sql.replace("GMF_SJ", "(GMF_SJ").replace("GMF_EMAIL" + ":" + GMF_EMAIL, "GMF_EMAIL" + ":" + GMF_EMAIL + ")");
        }
        if (!KPRQ_Q.isEmpty() && !KPRQ_Z.isEmpty()) {
            sql = sql + " AND" + solrSql1;
        }
        log.info("solr_sql：" + sql);
        List<String> solrList = null;
        if (KPRQ_Q.equals("") && KPRQ_Z.equals("")) {
            response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS, sql, pageNo, pageSize, " KPRQ:[" + "20150801000000" + " TO " + "20250801000000" + "]");
            solrList = analyzeResponse(response);
            return solrList;
        }
        String collections = getCollections(KPRQ_Q, KPRQ_Z);
        response = getPageResponse(collections, sql, pageNo, pageSize, solrSql1);
        solrList = analyzeResponse(response);
        return solrList;
    }

    public List<String> queryByYDMandYHM2(String yfpdm, String yfphm, int pageNo, int pageSize)
            throws SolrServerException {
        QueryResponse response = null;
        StringBuilder params = new StringBuilder("YFP_DM : " + yfpdm + " AND YFP_HM : " + yfphm);
        response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS, params.toString(), pageNo, pageSize, "");
        return analyzeResponse(response);
    }

    public List<String> queryByDSPTBMandDDH2(String[] dsptbm, String ddh, int pageNo, int pageSize)
            throws SolrServerException {

        QueryResponse response = null;

//        StringBuilder params = new StringBuilder("( DSPTBM : ");
//
//        for (int i = 0; i < dsptbm.length; i++) {
//            if (i == dsptbm.length - 1) {
//                params.append(dsptbm[i].trim() + ") AND DDH:" + ddh);
//            } else {
//                params.append(dsptbm[i].trim() + " OR DSPTBM :");
//            }
//        }
//		response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS,params.toString(), pageNo, pageSize, "");

        StringBuilder params = new StringBuilder("( DSPTBM : ");
        for (int i = 0; i < dsptbm.length; i++) {
            if (i == dsptbm.length - 1) {
                params.append(dsptbm[i].trim() + " )");
            } else {
                params.append(dsptbm[i].trim() + " OR DSPTBM : ");
            }
        }
        String sql = "DDH:" + ddh;
        response = getPageResponse(ClusterSolrDao.DEFAULT_COLLECTIONS, sql, pageNo, pageSize, params.toString());

        List<String> solrList = analyzeResponse(response);
        return solrList;

    }

}
