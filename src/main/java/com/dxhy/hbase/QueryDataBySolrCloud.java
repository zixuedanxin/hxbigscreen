package com.dxhy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过solr过滤查询hbase数据，查询solr指定页码指定条数据
 */
public class QueryDataBySolrCloud {

    private static CloudSolrServer server; // 只创建一个SolrServer实例
    private static String zkHost = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181/solr";
    private static String defaultCollection = "test_invoice";
    private static SolrQuery query;
    /**
     * @param args
     * @throws SolrServerException 
     * @throws IOException 
     */
    public static void main(String[] args) throws SolrServerException, IOException {


        final Configuration conf;
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181");
        HTable table = new HTable(conf, "test:Invoice");
        Get get = null;
        List<Get> list = new ArrayList<Get>();

        server = new CloudSolrServer(zkHost);
        server.setDefaultCollection(defaultCollection);
        server.connect();

        query = new SolrQuery("KPRQ:20161216000000");
        setPageNoAndPageSize(9,10);
        QueryResponse response = server.query(query);

        SolrDocumentList docs = response.getResults();
        System.out.println("文档个数：" + docs.getNumFound()); //数据总条数也可轻易获取
        System.out.println("查询时间：" + response.getQTime()); 
        for (SolrDocument doc : docs) {
            //System.out.println(((String) doc.getFieldValue("id")));
            get = new Get(Bytes.toBytes((String) doc.getFieldValue("id")));
            list.add(get);
        }
        
        Result[] res = table.get(list);
        
        byte[] bt1 = null;
        String str1 = null;
        for (Result rs : res) {
            bt1 = rs.getValue("Info".getBytes(), "Data".getBytes());
            if (bt1 != null && bt1.length>0) {str1 = new String(bt1);} else {str1 = "无数据";} //对空值进行new String的话会抛出异常
            System.out.print(new String(rs.getRow()) + "--");
            System.out.println(str1);
        }
        table.close();
        server.shutdown();
    }

    private static void setPageNoAndPageSize(int pageNo, long pageSize) {

        if (pageNo > 0) {
            query.setStart((int) pageSize * (pageNo - 1));
        }
        if (pageSize > 0) {
            query.setRows((int) pageSize);
        }
    }
}