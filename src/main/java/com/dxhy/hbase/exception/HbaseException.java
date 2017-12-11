package com.dxhy.hbase.exception;

/**
 * Created by thinkpad on 2017/3/2.
 */
public class HbaseException extends Exception{
    private String retCd ;  //异常对应的返回码
    private String msgDes;  //异常对应的描述信息

    public HbaseException() {
        super();
    }

    public HbaseException(String message) {
        super(message);
        msgDes = message;
    }

    public HbaseException(String retCd, String msgDes) {
        super();
        this.retCd = retCd;
        this.msgDes = msgDes;
    }

    public String getRetCd() {
        return retCd;
    }

    public String getMsgDes() {
        return msgDes;
    }
}
