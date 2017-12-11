package com.dxhy.constants;

public class HBaseConstants {
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 5242880;

    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";

    public static final boolean DEFAULT_AUTO_FLUSH = true;

    public static final String ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

    public static final String DEFAULT_ZOOKEEPER_CLIENT_PORT = "2181";

    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

    public static final String HBASE_MASTER = "hbase.master";

    //***********************hbase table****************************
    public static final String INVOICE_TABLE_NAME = "tableName";

    public static final String FILECONTENT_TABLE_NAME = "fileContentTableName";

    public static final String BATCH_SIZE = "batchsize";

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String INVOICE_FAMILY = "Info";

}
