package com.dxhy.solr;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrServer;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SolrCollectionPool implements Serializable {
    private static Logger log = Logger.getLogger(SolrCollectionPool.class);
    private static int zkClientTimeout = 20000;
    private static int zkConnectTimeout = 1000;
    public static SolrCollectionPool instance = new SolrCollectionPool();  
    private static Map<String, BlockingQueue<CloudSolrServer>> poolMap = new ConcurrentHashMap<String, BlockingQueue<CloudSolrServer>>();
    public SolrCollectionPool() {  
  
    }  
    public synchronized BlockingQueue<CloudSolrServer> getCollectionPool(String zkHost, String collection, final int size) {  
        if (poolMap.get(collection) == null) {  
            log.info("solr:" + collection + " poolsize:" + size);  
            System.setProperty("javax.xml.parsers.DocumentBuilderFactory",  
                    "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");  
            System.setProperty("javax.xml.parsers.SAXParserFactory",  
                    "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");  
            BlockingQueue<CloudSolrServer> serverList = new LinkedBlockingQueue<CloudSolrServer>(size);
            for (int i = 0; i < size; i++) {  
                CloudSolrServer cloudServer = new CloudSolrServer(zkHost);  
                cloudServer.setDefaultCollection(collection);  
                cloudServer.setZkClientTimeout(zkClientTimeout);
                cloudServer.setZkConnectTimeout(zkConnectTimeout);
                cloudServer.connect();  
                serverList.add(cloudServer);  
            }  
            poolMap.put(collection, serverList);  
        }  
        return poolMap.get(collection);  
    }  
  
    public static SolrCollectionPool instance() {  
        return SolrCollectionPool.instance;  
    }  
}  