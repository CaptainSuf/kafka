package com.dev;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyNewProducter extends Thread {

    private String topic;

    public MyNewProducter(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        KafkaProducer kafkaProducer = createProduct();
        ProducerRecord record;
        for(int i=0;i<10;i++){
            record = new ProducerRecord(topic,"kafkaProducter message："+i);
            kafkaProducer.send(record);
        }
    }

    private KafkaProducer createProduct(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        return new KafkaProducer(properties);
    }

    public static void main(String[] args){
        new Thread(new MyNewProducter("test")).start();
    }
}
