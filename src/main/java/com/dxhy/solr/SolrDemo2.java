package com.dxhy.solr;

import com.dxhy.entity.Invoice4Solr;
import com.dxhy.solr.dao.ClusterSolrDao;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.dxhy.solr.dao.BaseSolrDao.generateInvoice4Solr;

/**
 * Created by drguo on 2017/12/2.
 */
public class SolrDemo2 {


    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        File file = new File("test_data/part-m-00059");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String strJson = null;
            // 一次读入一行，直到读入null为文件结束
            while ((strJson = reader.readLine()) != null) {
                Invoice4Solr invoice4Solr = generateInvoice4Solr(strJson);
                ClusterSolrDao.saveServer.addBean(invoice4Solr);
            }
            ClusterSolrDao.saveServer.commit();
            System.out.println("cost:" + (System.currentTimeMillis() - start) + "ms.");//13184条 cost:147174ms.
            System.out.println("cost:" + (System.currentTimeMillis() - start)/60000.0 + "mins.");//cost:2.4529mins.
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
        ClusterSolrDao.saveServer.shutdown();
    }
}
