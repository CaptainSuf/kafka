package com.dev;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class MyNewConsumer extends Thread{

    private String topic;

    public MyNewConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        Consumer kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String,String> records;
        while (true){
            records = kafkaConsumer.poll(0);
            if(records.count()>0){
                for(ConsumerRecord record:records){
                    System.out.println("收到消息："+record.value());
                }
            }
        }
    }

    private KafkaConsumer createConsumer(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.99.100:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"100");
        return new KafkaConsumer(properties);
    }

    public static void main(String[] args) {
        new Thread(new MyNewConsumer("test")).start();
    }
}
