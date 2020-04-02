package com.huyong.bigdata.kafka;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by yonghu on 2020/3/12.
 */
public class InterceptorProducer {
    public static void main(String[]  args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", MQDict.MQ_ADDRESS_COLLECTION);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
    }
}
