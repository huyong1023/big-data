package com.huyong.bigdata.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by yonghu on 2020/3/12.
 */
public class TimeInterceptor implements ProducerInterceptor {
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {

        return new ProducerRecord(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), System.currentTimeMillis() + "," + producerRecord.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
