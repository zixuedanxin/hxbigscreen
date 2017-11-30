package com.dxhy.solr.dao;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ClusterSolrDao {
	
    public static Properties props;

    public static String ZK_SERVER; // cloud模式zookeeper服务器地址

    public static String COLLECTION; // solr集合

    public static String[] COLLECTIONS; // solr所有集合

    public static String DEFAULT_COLLECTIONS;

    public static String COLLECTION_PREFIX; //collection前缀

    public static CloudSolrServer saveServer;

    public static CloudSolrServer queryServer;

    static {
        props = new Properties();
        try {
            InputStream in = BaseSolrDao.class.getResourceAsStream("/solr.properties");
            props.load(in);
            ZK_SERVER = props.getProperty("ZK_SERVER");
            COLLECTION = props.getProperty("COLLECTION");
            COLLECTIONS = props.getProperty("COLLECTIONS").split(",");
            COLLECTION_PREFIX = props.getProperty("COLLECTION_PREFIX");
            DEFAULT_COLLECTIONS = props.getProperty("COLLECTIONS");

            saveServer = new CloudSolrServer(ZK_SERVER,false);
            saveServer.setDefaultCollection(COLLECTION);

            queryServer = new CloudSolrServer(ZK_SERVER,false);
        } catch (IOException e) {
            throw new RuntimeException("加载配置文件出错");
        } catch (NullPointerException e) {
            throw new RuntimeException("文件不存在");
        }
    }


}
