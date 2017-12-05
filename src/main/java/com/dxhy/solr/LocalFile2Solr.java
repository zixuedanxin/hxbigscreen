package com.dxhy.solr;

import com.dxhy.entity.Invoice4Solr;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.dxhy.solr.dao.BaseSolrDao.generateInvoice4Solr;

/**
 * Created by drguo on 2017/12/2.
 */
public class LocalFile2Solr {

    private static CloudSolrServer cloudSolrServer;
    private static String zkHost = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181/solr";
    private static String defaultCollection = "test_invoice2";
    private static int zkClientTimeout = 20000;
    private static int zkConnectTimeout = 1000;

    private static synchronized CloudSolrServer getCloudSolrServer(final String zkHost) {
        if (cloudSolrServer == null) {
            try {
                cloudSolrServer = new CloudSolrServer(zkHost);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cloudSolrServer;
    }

    public static void run(){

        CloudSolrServer cloudSolrServer = getCloudSolrServer(zkHost);

        cloudSolrServer.setDefaultCollection(defaultCollection);
        cloudSolrServer.setZkClientTimeout(zkClientTimeout);
        cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrServer.connect();

        long start = System.currentTimeMillis();
        File file = new File("test_data/part-m-00059");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String strJson = null;
            // 一次读入一行，直到读入null为文件结束
            while ((strJson = reader.readLine()) != null) {
                Invoice4Solr invoice4Solr = generateInvoice4Solr(strJson);
                cloudSolrServer.addBean(invoice4Solr);
            }
            cloudSolrServer.commit();//cost:109509ms  1.8mins 13184条 3.46mins
            System.out.println("cost:" + (System.currentTimeMillis() - start) + "ms.");
            System.out.println("cost:" + (System.currentTimeMillis() - start)/60000.0 + "mins.");
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

        // release the resource
        cloudSolrServer.shutdown();

    }

    public static void main(String[] args) {
        run();
    }

}
