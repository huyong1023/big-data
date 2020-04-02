package com.huyong.bigdata.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by yonghu on 2020/3/12.
 */
public class ConterInterceptor implements ProducerInterceptor {
    private int errorCounter = 0;
    private int successCounter = 0;



    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
