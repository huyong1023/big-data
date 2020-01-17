package com.huyong.bigdata.hadoop.order;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by yonghu on 2020/1/17.
 */
public class OrderBean extends WritableComparator {

    private String orderId;
    private double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }



}
