package com.dxhy.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dxhy.hbase.exception.HbaseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;
import com.dxhy.constants.HBaseConstants;
import com.dxhy.constants.HBaseParams;
import com.dxhy.constants.InvoiceConstants;
import com.dxhy.entity.Invoice;
import com.dxhy.util.HBaseUtil;
import com.dxhy.util.MD5Util;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class BaseHBaseDao {

    public static Logger logger = getLogger(BaseHBaseDao.class);

    public static HConnectionManager manager = null;

    public static HConnection connection = null;

    public static Configuration configuration = HBaseConfiguration.create();


    static {
        configuration.set(HBaseConstants.ZOOKEEPER_CLIENT_PORT, HBaseParams.ZOOKEEPER_CLIENT_PORT_VALUE);
        configuration.set(HBaseConstants.HBASE_ZOOKEEPER_QUORUM, HBaseParams.HBASE_ZOOKEEPER_QUORUM_VALUE);
        configuration.set(HBaseConstants.HBASE_MASTER, HBaseParams.HBASE_MASTER_VALUE);
        try {
            connection = HConnectionManager.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("创建connection失败！");
        }
    }


    public static HTableInterface getTable(String tableName) throws HbaseException {

        HTableInterface table = null;
        table = getTable(tableName, HBaseConstants.DEFAULT_AUTO_FLUSH, HBaseConstants.DEFAULT_WRITE_BUFFER_SIZE);
        return table;

    }

    public static synchronized HTableInterface getTable(String tableName, boolean autoFlush, int writeBufferSize) throws HbaseException {

        try {
            HTableInterface table = connection.getTable(tableName);
            table.setAutoFlushTo(autoFlush);
            table.setWriteBufferSize(writeBufferSize);
            return table;
        } catch (IOException e) {
            e.printStackTrace();
            throw new HbaseException("RC0151", "调用com.dxhy.hbase.dao.BaseHBaseDao.getTable方法 - 获取hbase数据库连接失败");
        }

    }

    public static void close(HTableInterface table) throws IOException {

        table.close();

    }

    public void dropTable(String tablename) {

        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            logger.error("master is not running!");
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            logger.error("the failure of zookeeper connection!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public List<Get> generateGets(List<String> solrList) {

        List<Get> gets = new ArrayList<Get>();
        if (solrList.size() > 1) {
            for (int i = 1; i < solrList.size(); i++) {
                Get get = new Get(solrList.get(i).getBytes());
                gets.add(get);
            }
            return gets;
        } else {
            return null;
        }
    }

    public List<Invoice> queryByBatch(HTableInterface table, List<Get> gets, int totalPage, boolean fp_kjmx) throws IOException, HbaseException {

        List<Invoice> invoiceList = new ArrayList<Invoice>();
        JSONObject json = new JSONObject();
        if (gets == null) {
            throw new HbaseException("RC0000", "调用com.dxhy.hbase.dao.BaseHBaseDao.queryByBatch方法  " +
                    "success,hbase查询无返回结果");
        } else {
            Result[] rs = table.get(gets);
            if (rs.length == 1 && rs[0].isEmpty()) {
                throw new HbaseException("RC0000", "调用com.dxhy.hbase.dao.BaseHBaseDao.queryByBatch方法  " +
                        "success,hbase查询无返回结果");
            }
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    json.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
                }
                if (fp_kjmx == true) {
                    Invoice invoice = HBaseUtil.setProperties(json, Invoice.class);
                    invoice.setTotal(totalPage);
                    invoice.setData(JSONObject.parseObject(invoice.getData()).getString(InvoiceConstants.DATA));
                    invoiceList.add(invoice);
                } else {
                    Invoice invoice = HBaseUtil.setProperties(json, Invoice.class);
                    invoice.setTotal(totalPage);
                    JSONObject fpkjObject = new JSONObject();
                    fpkjObject.put(InvoiceConstants.FP_KJ, JSONObject.parseObject(invoice.getData())
                            .getJSONObject(InvoiceConstants.DATA).getJSONObject(InvoiceConstants.FP_KJ));
                    invoice.setData(fpkjObject.toJSONString());
                    invoiceList.add(invoice);
                }
            }
        }

        return invoiceList;
    }

    public static Put generatePut(String json) {

        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject dataObject = jsonObject.getJSONObject(InvoiceConstants.DATA);
        JSONObject fpkj = dataObject.getJSONObject(InvoiceConstants.FP_KJ);
        String cover = jsonObject.getString(InvoiceConstants.COVER);
        String type = jsonObject.getString(InvoiceConstants.TYPE);
        String addTime = String.valueOf(System.currentTimeMillis());
        //fpkj.remove(InvoiceConstants.FILE_CONTENT);
        String kprq = fpkj.getString(InvoiceConstants.KPRQ);
        String fpdm = fpkj.getString(InvoiceConstants.FP_DM);
        String fphm = fpkj.getString(InvoiceConstants.FP_HM);
        String rowKey = MD5Util.MD5Encode(fpdm + fphm);

        Put put = new Put(Bytes.toBytes(rowKey));
        //addColumn(byte[] family, byte[] qualifier, byte[] value)
        put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                Bytes.toBytes(InvoiceConstants.KPRQ), Bytes.toBytes(kprq));
        put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                Bytes.toBytes(InvoiceConstants.TYPE), Bytes.toBytes(type));
        put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                Bytes.toBytes(InvoiceConstants.COVER), Bytes.toBytes(cover));
        put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                Bytes.toBytes(InvoiceConstants.DATA), Bytes.toBytes(jsonObject.toJSONString()));
        put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                Bytes.toBytes(InvoiceConstants.ADDTIME), Bytes.toBytes(addTime));
        return put;
    }

    public Put generateFileContentPut(String json) {

        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject dataObject = jsonObject.getJSONObject(InvoiceConstants.DATA);
        JSONObject fpkj = dataObject.getJSONObject(InvoiceConstants.FP_KJ);
        String kprq = fpkj.getString(InvoiceConstants.KPRQ);
        String fpdm = fpkj.getString(InvoiceConstants.FP_DM);
        String fphm = fpkj.getString(InvoiceConstants.FP_HM);
        String fileContent = fpkj.getString(InvoiceConstants.FILE_CONTENT);
        String rowKey = MD5Util.MD5Encode(fpdm + fphm);

        if (fileContent != null && !fileContent.equals("")) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                    Bytes.toBytes(InvoiceConstants.FILE_CONTENT), Bytes.toBytes(fileContent));
            put.addColumn(Bytes.toBytes(HBaseConstants.INVOICE_FAMILY),
                    Bytes.toBytes(InvoiceConstants.KPRQ), Bytes.toBytes(kprq));
            return put;

        }

        return null;
    }

    public static void saveInvoice(HTableInterface table, Put put) throws HbaseException {

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HbaseException("RC0151", "调用com.dxhy.hbase.dao.BaseHBaseDao.saveInvoice方法  IO异常");
        }

    }

}
