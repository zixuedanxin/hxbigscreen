package com.dxhy.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.dxhy.entity.Invoice4Solr;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;

import static com.dxhy.solr.dao.BaseSolrDao.generateInvoice4Solr;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by drguo on 2017/11/30.
 */
public class SolrDemo {

    private static final Logger logger = getLogger(SolrDemo.class);

    private static CloudSolrServer cloudSolrServer;

    private static synchronized CloudSolrServer getCloudSolrServer(final String zkHost) {
        if (cloudSolrServer == null) {
            try {
                cloudSolrServer = new CloudSolrServer(zkHost);
            } catch (Exception e) {
                logger.error("The URL of zkHost is not correct!");
                e.printStackTrace();
            }
        }
        return cloudSolrServer;
    }

    private void addIndex(SolrServer solrServer) {
        try {
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.addField("id", "1");
            doc1.addField("name", "tom");
            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.addField("id", "2");
            doc2.addField("name", "jack");
            SolrInputDocument doc3 = new SolrInputDocument();
            doc3.addField("id", "3");
            doc3.addField("name", "rose");
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            docs.add(doc1);
            docs.add(doc2);
            docs.add(doc3);
            solrServer.add(docs);
            solrServer.commit();
        } catch (SolrServerException e) {
            logger.error("Add docs Exception");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void search(SolrServer solrServer, String strQuery) {
        SolrQuery query = new SolrQuery();
        query.setQuery(strQuery);
        try {
            QueryResponse response = solrServer.query(query);
            SolrDocumentList docs = response.getResults();
            System.out.println("文档个数：" + docs.getNumFound());
            for (SolrDocument doc : docs) {
                String id = (String) doc.getFieldValue("id");
                Long version = (Long) doc.getFieldValue("_version_");
                System.out.println("id: " + id);
                System.out.println("_version_: " + version);
                //String kprq = (String) doc.getFieldValue("KPRQ");
                //System.out.println("kprq: " + kprq);
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteAllIndex(SolrServer solrServer) {
        try {
            solrServer.deleteByQuery("*:*");
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        final String zkHost = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181/solr";
        final String defaultCollection = "test_invoice2";
        final int zkClientTimeout = 20000;
        final int zkConnectTimeout = 1000;
        CloudSolrServer cloudSolrServer = getCloudSolrServer(zkHost);

        cloudSolrServer.setDefaultCollection(defaultCollection);
        cloudSolrServer.setZkClientTimeout(zkClientTimeout);
        cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrServer.connect();

        String strJson = "{\"Cover\":1,\"Data\":{\"FP_KJ\":{\"BZ\":\"\",\"CH_BZ\":\"9\",\"CXCS\":\"1\",\"CYRZZT\":\"9\",\"DDH\":\"9847390367\",\"DDSJ\":\"20150809190026\",\"DSPTBM\":\"11110101\",\"EWM\":\"iVBORw0KGgoAAAANSUhEUgAAAIsAAACLCAIAAADee/2oAAADmUlEQVR42u3cYXKrMAxF4ex/0+kC Oh1A91wbyuEnkwTsj4dkyX2fr8e9j49T8AyhT3acutLf36Iu8fsHh5Myup/ZuA4voZBCCrFC+ERc muvZjMw+MztzaRKQWVVIIYVKQmcm69KQ8MAWXjQcRfjh8/ejkEIK3VMoZLiUms/uOXyYwlCnkEIK PV2IChK9FfuCJ08hhRS6s9Aszd17JlzVz4IWXsBVSCGFqkK9N/ibz9yog+cZhd4qhG1MyRbzYa6P d7DCOim510chhRTKhHo1UGrS8TvEJ7Q0hwoppNBGoXDjy8qK58qCCDV2hRRSaLtQ+JYPO8pU83tv Hr8tU1BIobcK9TJpqkixpcpALTkG96OQQgotE1rQEQn3zeDtnFmOTn1LIYUUAoXw+ubsM+FDgO/R 6fWHzsczhRRSqC00G0n4cp/9DpWs92LwvKagkEIK1Sqn1Gr8iQ3y8FthsVghhRRqCFFvZ3ylHcah MH2nLkFWfRRSSKFRXS7srMyG3eumL0i7w0h5eFGFFFIIEQoHQLWf8WhB7cjZ2DJXSCGFEKHZPM6y ZPwh6IUxqpCRrBkUUkghVmjjC/e75I9bqLyZKtcenlFIIYVYIbwYGoYWKhWmGKj+2fmRKqSQQohQ 6R169QhbUGHejKfmSGVEIYUUYoWoAfQCAB6H8H1FbMtcIYUUWi9EFUN7GXkYRGe1EvzrCimkUFWo V8QME3F8nY+n1GwIV0ghhTYK4d3iUBFvUVPdoGTICimk0EYhHI9a+YdlzZVbjg5/WSGFFGoLhWUC akNPuO2Gysh7T5VCCilUFbpbgwff2TMLmVuKFAoppFBDCM9lwyHhcQhnwAvBZOVUIYUU+iW0cS/L l/5vVvJ57FVOBzOmkEIKIUK9vTUL+ihhQaS3nAirtAoppBAoFHZ6qJc71XTB4xDeDTo/LQoppFBJ iC3/NWqO4R2uLCUAe30UUkghSKiX74ZN4lnST9VSZ4EEeSwUUkghRChsw+B96F4lYhY78Ufn/D8A hRRSCBGijpX7ZhYErTCsItUKhRRSCBFa0LOhOBfExb07AA4yBYUUUoheseJDokqW+Hs/nNCSokIK KVQSorLSBWa9SDnrcFMpvkIKKfRvhC5ddEGLehY28FKsQgop9Aih2au8N7PUhh48Bm+LQwop9FYh 6r2/YKvQ3drz4cOkkEIKVYXwhTEeq6jAFsaqcKkwrykopJBCIyGP2x4K3f34AZGAPktdk3IxAAAA AElFTkSuQmCC\",\"FHR\":\"\",\"FILE_PATH\":\"/data1/sdb/vat/pdf/11110101/2015/8/15/11100157107101494104.pdf\",\"FPLB\":\"9\",\"FPQQLSH\":\"11100157107101494104\",\"FPZL_DM\":\"\",\"FPZT\":\"9\",\"FP_DM\":\"044001500111\",\"FP_HM\":\"1\",\"FWMW\":\"><>4/587<<21>8-31*>6+/>97**25/>8488424+14*382*0>2*0/>1>>//8<5+/460>4<4708354>*+->97**2*4>8488424+14*382*6<9*\",\"GMFQYLX\":\"03\",\"GMF_DZDH\":\"18636924682\",\"GMF_EMAIL\":\"\",\"GMF_MC\":\"购货方--公司个人\",\"GMF_NSRSBH\":\"\",\"GMF_SJ\":\"18636924682\",\"GMF_WX\":\"9999\",\"GMF_YHZH\":\"\",\"HJJE\":\"14.68\",\"HJSE\":\"1.91\",\"HY_DM\":\"5177\",\"HY_MC\":\"测试\",\"JSHJ\":\"16.59\",\"JYM\":\"69640458420485568535\",\"KPLX\":\"1\",\"KPR\":\"京东商城\",\"KPRQ\":\"20170909190026\",\"KPXM\":\"北纯 非转基因 东北杂粮 黑芝麻320g（真空装）\",\"KPXMSL\":\"2\",\"KPZT\":\"99\",\"QDXMMC\":\"9999\",\"QD_BZ\":\"9\",\"SKR\":\"京东商城\",\"SSYF\":\"\",\"SWJG_DM\":\"111019201\",\"SZQM\":\"\",\"TSCHBZ\":\"\",\"XHQD\":\"\",\"XHQDBZ\":\"\",\"XSF_DZDH\":\"北京市北京经济技术开发区科创十四街99号2号楼B168室,010-56754036\",\"XSF_MC\":\"北京京东世纪贸易有限公司\",\"XSF_NSRSBH\":\"110192562134916\",\"XSF_YHZH\":\"\",\"YFP_DM\":\"\",\"YFP_HM\":\"\"},\"FP_KJMX\":[{\"BZ\":\"9999\",\"DW\":\"\",\"GGXH\":\"无\",\"JLDW_DM\":\"9999\",\"KPXMXH\":\"1\",\"SE\":\"6.52\",\"SL\":\"0.13\",\"SM\":\"\",\"SM_DM\":\"9999\",\"XMBM\":\"\",\"XMDJ\":\"16.72566372\",\"XMJE\":\"50.18\",\"XMMC\":\"北纯 非转基因 东北杂粮 黑芝麻320g（真空装）\",\"XMSL\":\"3.0\",\"ZK\":\"0.00\"},{\"BZ\":\"9999\",\"DW\":\"\",\"GGXH\":\"\",\"JLDW_DM\":\"9999\",\"KPXMXH\":\"2\",\"SE\":\"-4.61\",\"SL\":\"0.13\",\"SM\":\"\",\"SM_DM\":\"9999\",\"XMBM\":\"\",\"XMDJ\":\"-35.49557522\",\"XMJE\":\"-35.5\",\"XMMC\":\"折扣(70.741%)\",\"XMSL\":\"1.0\",\"ZK\":\"0.00\"}]},\"Type\":0}";

        Invoice4Solr invoice4Solr = generateInvoice4Solr(strJson);

        try {
            cloudSolrServer.addBean(invoice4Solr);
            cloudSolrServer.commit();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }


        //测试实例！
        SolrDemo test = new SolrDemo();
        //添加index
        //test.addIndex(cloudSolrServer);

        //test.search(cloudSolrServer, "id:1");

        //test.deleteAllIndex(cloudSolrServer);

        //System.out.println("after delete all");
        test.search(cloudSolrServer, "*:*");

        // release the resource
        cloudSolrServer.shutdown();

    }
}
