package com.atguigu.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingCompartor extends WritableComparator {

	// дһ���ղι���
	public OrderGroupingCompartor(){
		super(OrderBean.class, true);
	}
	
	// ��д�Ƚϵķ���
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;

		// ���ݶ���id�űȽϣ��ж��Ƿ���һ��
		return aBean.getOrderId().compareTo(bBean.getOrderId());
	}
}
