package com.dxhy.constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.dxhy.util.HBaseUtil;

public class HBaseParams {


    public static int WRITE_BUFFER_SIZE_VALUE;

    public static String ZOOKEEPER_CLIENT_PORT_VALUE = null;

    public static String HBASE_ZOOKEEPER_QUORUM_VALUE = null;

    public static String HBASE_MASTER_VALUE = null;

    public static String INVOICE_TABLE_NAME_VALUE = null;

    public static String FILECONTENT_TABLE_NAME_VALUE = null;

    public static int BATCH_SIZE_VALUE;

    static {
        InputStream in = HBaseUtil.class.getResourceAsStream("/hbase.properties");
        Properties props = new Properties();
        try {
            props.load(in);
            HBaseParams.HBASE_MASTER_VALUE = props.getProperty(HBaseConstants.HBASE_MASTER);
            HBaseParams.WRITE_BUFFER_SIZE_VALUE = Integer.parseInt(props.getProperty(HBaseConstants.WRITE_BUFFER_SIZE));
            HBaseParams.HBASE_ZOOKEEPER_QUORUM_VALUE = props.getProperty(HBaseConstants.HBASE_ZOOKEEPER_QUORUM);
            HBaseParams.INVOICE_TABLE_NAME_VALUE = props.getProperty(HBaseConstants.INVOICE_TABLE_NAME);
            HBaseParams.FILECONTENT_TABLE_NAME_VALUE = props.getProperty(HBaseConstants.FILECONTENT_TABLE_NAME);
            HBaseParams.ZOOKEEPER_CLIENT_PORT_VALUE = props.getProperty(HBaseConstants.ZOOKEEPER_CLIENT_PORT);
            HBaseParams.BATCH_SIZE_VALUE = Integer.parseInt(props.getProperty(HBaseConstants.BATCH_SIZE));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
