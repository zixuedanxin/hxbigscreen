package com.dxhy.entity;

public class Invoice {
    private String Data;
    private String KPRQ;
    private long addTime;
    private int Cover;
    private int Type;
    private int total;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public String getKPRQ() {
        return this.KPRQ;
    }

    public void setKPRQ(String paramString) {
        this.KPRQ = paramString;
    }

    public long getAddTime() {
        return this.addTime;
    }

    public void setAddTime(long paramLong) {
        this.addTime = paramLong;
    }

    public int getCover() {
        return this.Cover;
    }

    public void setCover(int paramInt) {
        this.Cover = paramInt;
    }

    public String getData() {
        return this.Data;
    }

    public void setData(String paramString) {
        this.Data = paramString;
    }

    public int getType() {
        return this.Type;
    }

    public void setType(int paramInt) {
        this.Type = paramInt;
    }

}
